// @ts-nocheck

import {
	all,
	alloc,
	decodeBytes,
	encodeBytes,
	entries,
	fromEntries,
	fromJson,
	isBrowser,
	keys,
	libName,
	mkErr,
	noOp,
	toJson
} from './utils';

const TypedArray = Object.getPrototypeOf(Uint8Array);
const typeByteLimit = 12;
const typeIndex = 0;
const nonceIndex = typeIndex + typeByteLimit;
const tagIndex = nonceIndex + 1;
const progressIndex = tagIndex + 1;
const payloadIndex = progressIndex + 1;
const chunkSize = 16 * 2 ** 10 - payloadIndex;
const oneByteMax = 0xff;
const buffLowEvent = 'bufferedamountlow';
const internalNs = (ns: string) => '@_' + ns;

export default (
	onPeer: (peer: any, id: string) => void,
	onPeerLeave: (id: string) => void,
	onSelfLeave: () => void
) => {
	const peerMap: Record<string, any> = {};
	const actions: Record<string, any> = {};
	const actionsCache: Record<string, any> = {};
	const pendingTransmissions: Record<string, any> = {};
	const pendingPongs: Record<string, () => void> = {};
	const pendingStreamMetas: Record<string, unknown> = {};
	const pendingTrackMetas: Record<string, unknown> = {};
	const listeners = {
		onPeerJoin: noOp,
		onPeerLeave: noOp,
		onPeerStream: noOp,
		onPeerTrack: noOp
	};

	const iterate = (targets: string | string[] | undefined, f: (id: string, peer: any) => any) =>
		(targets ? (Array.isArray(targets) ? targets : [targets]) : keys(peerMap)).flatMap((id) => {
			const peer = peerMap[id];

			if (!peer) {
				console.warn(`${libName}: no peer with id ${id} found`);
				return [];
			}

			return f(id, peer);
		});

	const exitPeer = (id: string) => {
		if (!peerMap[id]) {
			return;
		}

		peerMap[id].destroy();
		delete peerMap[id];
		delete pendingTransmissions[id];
		delete pendingPongs[id];
		listeners.onPeerLeave(id);
		onPeerLeave(id);
	};

	const makeAction = (type: string) => {
		if (actions[type]) {
			return actionsCache[type];
		}

		if (!type) {
			throw mkErr('action type argument is required');
		}

		const typeBytes = encodeBytes(type);

		if (typeBytes.byteLength > typeByteLimit) {
			throw mkErr(
				`action type string "${type}" (${typeBytes.byteLength}b) exceeds ` +
					`byte limit (${typeByteLimit}). Hint: choose a shorter name.`
			);
		}

		const typeBytesPadded = new Uint8Array(typeByteLimit);
		typeBytesPadded.set(typeBytes);

		let nonce = 0;

		actions[type] = {
			onComplete: noOp,
			onProgress: noOp,

			setOnComplete: (f: (data: unknown, peerId: string, meta?: unknown) => void) =>
				(actions[type] = { ...actions[type], onComplete: f }),

			setOnProgress: (f: (progress: number, peerId: string, meta?: unknown) => void) =>
				(actions[type] = { ...actions[type], onProgress: f }),

			send: async (
				data: unknown,
				targets?: string | string[],
				meta?: unknown,
				onProgress?: (progress: number, peerId: string, meta?: unknown) => void
			) => {
				if (meta && typeof meta !== 'object') {
					throw mkErr('action meta argument must be an object');
				}

				const dataType = typeof data;

				if (dataType === 'undefined') {
					throw mkErr('action data cannot be undefined');
				}

				const isJson = dataType !== 'string';
				const isBlob = data instanceof Blob;
				const isBinary = isBlob || data instanceof ArrayBuffer || data instanceof TypedArray;

				if (meta && !isBinary) {
					throw mkErr('action meta argument can only be used with binary data');
				}

				const buffer = isBinary
					? new Uint8Array(isBlob ? await data.arrayBuffer() : (data as ArrayBuffer))
					: encodeBytes(isJson ? toJson(data) : (data as string));

				const metaEncoded = meta ? encodeBytes(toJson(meta)) : null;

				const chunkTotal = Math.ceil(buffer.byteLength / chunkSize) + (meta ? 1 : 0) || 1;

				const chunks = alloc(chunkTotal, (_, i) => {
					const isLast = i === chunkTotal - 1;
					const isMeta = meta && i === 0;
					const chunk = new Uint8Array(
						payloadIndex +
							(isMeta
								? metaEncoded.byteLength
								: isLast
									? buffer.byteLength - chunkSize * (chunkTotal - (meta ? 2 : 1))
									: chunkSize)
					);

					chunk.set(typeBytesPadded);
					chunk.set([nonce], nonceIndex);
					chunk.set([isLast | (isMeta << 1) | (isBinary << 2) | (isJson << 3)], tagIndex);
					chunk.set([Math.round(((i + 1) / chunkTotal) * oneByteMax)], progressIndex);
					chunk.set(
						meta
							? isMeta
								? metaEncoded
								: buffer.subarray((i - 1) * chunkSize, i * chunkSize)
							: buffer.subarray(i * chunkSize, (i + 1) * chunkSize),
						payloadIndex
					);

					return chunk;
				});

				nonce = (nonce + 1) & oneByteMax;

				return all(
					iterate(targets, async (id, peer) => {
						const { channel } = peer;
						let chunkN = 0;

						while (chunkN < chunkTotal) {
							const chunk = chunks[chunkN];

							if (channel.bufferedAmount > channel.bufferedAmountLowThreshold) {
								await new Promise((res) => {
									const next = () => {
										channel.removeEventListener(buffLowEvent, next);
										res(undefined);
									};

									channel.addEventListener(buffLowEvent, next);
								});
							}

							if (!peerMap[id]) {
								break;
							}

							peer.sendData(chunk);
							chunkN++;
							onProgress?.(chunk[progressIndex] / oneByteMax, id, meta);
						}
					})
				);
			}
		};

		return (actionsCache[type] ||= [
			actions[type].send,
			actions[type].setOnComplete,
			actions[type].setOnProgress
		]);
	};

	const handleData = (id: string, data: ArrayBuffer) => {
		const buffer = new Uint8Array(data);
		const type = decodeBytes(buffer.subarray(typeIndex, nonceIndex)).replaceAll('\x00', '');
		const [nonce] = buffer.subarray(nonceIndex, tagIndex);
		const [tag] = buffer.subarray(tagIndex, progressIndex);
		const [progress] = buffer.subarray(progressIndex, payloadIndex);
		const payload = buffer.subarray(payloadIndex);
		const isLast = !!(tag & 1);
		const isMeta = !!(tag & (1 << 1));
		const isBinary = !!(tag & (1 << 2));
		const isJson = !!(tag & (1 << 3));

		if (!actions[type]) {
			console.warn(`${libName}: received message with unregistered type (${type})`);
			return;
		}

		pendingTransmissions[id] ||= {};
		pendingTransmissions[id][type] ||= {};

		const target = (pendingTransmissions[id][type][nonce] ||= { chunks: [] });

		if (isMeta) {
			target.meta = fromJson(decodeBytes(payload));
		} else {
			target.chunks.push(payload);
		}

		actions[type].onProgress(progress / oneByteMax, id, target.meta);

		if (!isLast) {
			return;
		}

		const full = new Uint8Array(
			target.chunks.reduce((a: number, c: Uint8Array) => a + c.byteLength, 0)
		);

		target.chunks.reduce((a: number, c: Uint8Array) => {
			full.set(c, a);
			return a + c.byteLength;
		}, 0);

		delete pendingTransmissions[id][type][nonce];

		if (isBinary) {
			actions[type].onComplete(full, id, target.meta);
		} else {
			const text = decodeBytes(full);
			actions[type].onComplete(isJson ? fromJson(text) : text, id);
		}
	};

	const leave = async () => {
		await sendLeave('');
		await new Promise((res) => setTimeout(res, 99));
		entries(peerMap).forEach(([id, peer]) => {
			peer.destroy();
			delete peerMap[id];
		});
		onSelfLeave();
	};

	const [sendPing, getPing] = makeAction(internalNs('ping'));
	const [sendPong, getPong] = makeAction(internalNs('pong'));
	const [sendSignal, getSignal] = makeAction(internalNs('signal'));
	const [sendStreamMeta, getStreamMeta] = makeAction(internalNs('stream'));
	const [sendTrackMeta, getTrackMeta] = makeAction(internalNs('track'));
	const [sendLeave, getLeave] = makeAction(internalNs('leave'));

	onPeer((peer, id) => {
		if (peerMap[id]) {
			return;
		}

		peerMap[id] = peer;

		peer.setHandlers({
			data: (d) => handleData(id, d),
			stream: (stream: MediaStream) => {
				listeners.onPeerStream(stream, id, pendingStreamMetas[id]);
				delete pendingStreamMetas[id];
			},
			track: (track: MediaStreamTrack, stream: MediaStream) => {
				listeners.onPeerTrack(track, stream, id, pendingTrackMetas[id]);
				delete pendingTrackMetas[id];
			},
			signal: (sdp: unknown) => sendSignal(sdp, id),
			close: () => exitPeer(id),
			error: (err: unknown) => {
				console.error(err);
				exitPeer(id);
			}
		});

		listeners.onPeerJoin(id);
	});

	getPing((_, id) => sendPong('', id));

	getPong((_, id) => {
		pendingPongs[id]?.();
		delete pendingPongs[id];
	});

	getSignal((sdp, id) => peerMap[id]?.signal(sdp));

	getStreamMeta((meta, id) => (pendingStreamMetas[id] = meta));

	getTrackMeta((meta, id) => (pendingTrackMetas[id] = meta));

	getLeave((_, id) => exitPeer(id));

	if (isBrowser) {
		addEventListener('beforeunload', leave);
	}

	return {
		makeAction,

		leave,

		ping: async (id: string) => {
			if (!id) {
				throw mkErr('ping() must be called with target peer ID');
			}

			const start = Date.now();

			sendPing('', id);
			await new Promise((res) => (pendingPongs[id] = res));
			return Date.now() - start;
		},

		getPeers: () => fromEntries(entries(peerMap).map(([id, peer]) => [id, peer.connection])),

		addStream: (stream: MediaStream, targets?: string | string[], meta?: unknown) =>
			iterate(targets, async (id, peer) => {
				if (meta) {
					await sendStreamMeta(meta, id);
				}

				peer.addStream(stream);
			}),

		removeStream: (stream: MediaStream, targets?: string | string[]) =>
			iterate(targets, (_, peer) => peer.removeStream(stream)),

		addTrack: (
			track: MediaStreamTrack,
			stream: MediaStream,
			targets?: string | string[],
			meta?: unknown
		) =>
			iterate(targets, async (id, peer) => {
				if (meta) {
					await sendTrackMeta(meta, id);
				}

				peer.addTrack(track, stream);
			}),

		removeTrack: (track: MediaStreamTrack, targets?: string | string[]) =>
			iterate(targets, (_, peer) => peer.removeTrack(track)),

		replaceTrack: (
			oldTrack: MediaStreamTrack,
			newTrack: MediaStreamTrack,
			targets?: string | string[],
			meta?: unknown
		) =>
			iterate(targets, async (id, peer) => {
				if (meta) {
					await sendTrackMeta(meta, id);
				}

				peer.replaceTrack(oldTrack, newTrack);
			}),

		onPeerJoin: (f: (id: string) => void) => (listeners.onPeerJoin = f),

		onPeerLeave: (f: (id: string) => void) => (listeners.onPeerLeave = f),

		onPeerStream: (f: (stream: MediaStream, id: string, meta?: unknown) => void) =>
			(listeners.onPeerStream = f),

		onPeerTrack: (
			f: (track: MediaStreamTrack, stream: MediaStream, id: string, meta?: unknown) => void
		) => (listeners.onPeerTrack = f)
	};
};
