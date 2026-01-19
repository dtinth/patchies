// @ts-nocheck

import { decrypt, encrypt, genKey, sha1 } from './crypto';
import initPeer from './peer';
import room from './room';
import {
	all,
	alloc,
	fromJson,
	libName,
	mkErr,
	noOp,
	selfId,
	toJson,
	topicPath,
	watchOnline
} from './utils';

const poolSize = 20;
const announceIntervalMs = 5_333;
const offerTtl = 57_333;

export default ({ init, subscribe, announce }) => {
	const occupiedRooms: Record<string, Record<string, unknown>> = {};

	let didInit = false;
	let initPromises: Promise<unknown>[] = [];
	let offerPool: ReturnType<typeof initPeer>[] = [];
	let offerCleanupTimer: ReturnType<typeof setInterval> | undefined;
	let cleanupWatchOnline: () => void = noOp;

	return (
		config: { appId: string; password?: string },
		roomId: string,
		onJoinError?: (args: unknown) => void
	) => {
		const { appId } = config;

		if (occupiedRooms[appId]?.[roomId]) {
			return occupiedRooms[appId][roomId];
		}

		const pendingOffers: Record<string, Record<number, ReturnType<typeof initPeer>>> = {};
		const connectedPeers: Record<string, ReturnType<typeof initPeer>> = {};
		const rootTopicPlaintext = topicPath(libName, appId, roomId);
		const rootTopicP = sha1(rootTopicPlaintext);
		const selfTopicP = sha1(topicPath(rootTopicPlaintext, selfId));
		const key = genKey(config.password || '', appId, roomId);

		const withKey =
			(f: (key: unknown, sdp: string) => Promise<string>) =>
			async (signal: { type: string; sdp: string }) => ({
				type: signal.type,
				sdp: await f(key, signal.sdp)
			});

		const toPlain = withKey(decrypt);
		const toCipher = withKey(encrypt);

		const makeOffer = () => initPeer(true, config);

		const connectPeer = (peer: ReturnType<typeof initPeer>, peerId: string, relayId: number) => {
			if (connectedPeers[peerId]) {
				if (connectedPeers[peerId] !== peer) {
					peer.destroy();
				}
				return;
			}

			connectedPeers[peerId] = peer;
			onPeerConnect(peer, peerId);

			pendingOffers[peerId]?.forEach((pendingPeer, i) => {
				if (i !== relayId) {
					pendingPeer.destroy();
				}
			});
			delete pendingOffers[peerId];
		};

		const disconnectPeer = (peer: ReturnType<typeof initPeer>, peerId: string) => {
			if (connectedPeers[peerId] === peer) {
				delete connectedPeers[peerId];
			}
		};

		const prunePendingOffer = (peerId: string, relayId: number) => {
			if (connectedPeers[peerId]) {
				return;
			}

			const offer = pendingOffers[peerId]?.[relayId];

			if (offer) {
				delete pendingOffers[peerId][relayId];
				offer.destroy();
			}
		};

		const getOffers = (n: number) => {
			offerPool.push(...alloc(n, makeOffer));

			return all(
				offerPool
					.splice(0, n)
					.map((peer) => peer.offerPromise.then(toCipher).then((offer) => ({ peer, offer })))
			);
		};

		const handleJoinError = (peerId: string, sdpType: string) =>
			onJoinError?.({
				error: `incorrect password (${config.password}) when decrypting ${sdpType}`,
				appId,
				peerId,
				roomId
			});

		const handleMessage =
			(relayId: number) =>
			async (topic: string, msg: unknown, signalPeer: (topic: string, data: string) => void) => {
				const [rootTopic, selfTopic] = await all([rootTopicP, selfTopicP]);

				if (topic !== rootTopic && topic !== selfTopic) {
					return;
				}

				const { peerId, offer, answer, peer } =
					typeof msg === 'string' ? fromJson(msg) : (msg as Record<string, unknown>);

				if (peerId === selfId || connectedPeers[peerId]) {
					return;
				}

				if (peerId && !offer && !answer) {
					if (pendingOffers[peerId]?.[relayId]) {
						return;
					}

					const [[{ peer: offerPeer, offer: encryptedOffer }], topicHash] = await all([
						getOffers(1),
						sha1(topicPath(rootTopicPlaintext, peerId))
					]);

					pendingOffers[peerId] ||= {};
					pendingOffers[peerId][relayId] = offerPeer;

					setTimeout(() => prunePendingOffer(peerId, relayId), announceIntervals[relayId] * 0.9);

					offerPeer.setHandlers({
						connect: () => connectPeer(offerPeer, peerId, relayId),
						close: () => disconnectPeer(offerPeer, peerId)
					});

					signalPeer(topicHash, toJson({ peerId: selfId, offer: encryptedOffer }));
				} else if (offer) {
					const myOffer = pendingOffers[peerId]?.[relayId];

					if (myOffer && selfId > peerId) {
						return;
					}

					const peerConn = initPeer(false, config);
					peerConn.setHandlers({
						connect: () => connectPeer(peerConn, peerId, relayId),
						close: () => disconnectPeer(peerConn, peerId)
					});

					let plainOffer;

					try {
						plainOffer = await toPlain(offer);
					} catch {
						handleJoinError(peerId, 'offer');
						return;
					}

					if (peerConn.isDead) {
						return;
					}

					const [topicHash, answer] = await all([
						sha1(topicPath(rootTopicPlaintext, peerId)),
						peerConn.signal(plainOffer)
					]);

					signalPeer(topicHash, toJson({ peerId: selfId, answer: await toCipher(answer) }));
				} else if (answer) {
					let plainAnswer;

					try {
						plainAnswer = await toPlain(answer);
					} catch {
						handleJoinError(peerId, 'answer');
						return;
					}

					if (peer) {
						peer.setHandlers({
							connect: () => connectPeer(peer, peerId, relayId),
							close: () => disconnectPeer(peer, peerId)
						});

						peer.signal(plainAnswer);
					} else {
						const pendingPeer = pendingOffers[peerId]?.[relayId];

						if (pendingPeer && !pendingPeer.isDead) {
							pendingPeer.signal(plainAnswer);
						}
					}
				}
			};

		if (!config) {
			throw mkErr('requires a config map as the first argument');
		}

		if (!appId && !config.firebaseApp) {
			throw mkErr('config map is missing appId field');
		}

		if (!roomId) {
			throw mkErr('roomId argument required');
		}

		if (!didInit) {
			const initRes = init(config);
			offerPool = alloc(poolSize, makeOffer);
			initPromises = Array.isArray(initRes) ? initRes : [initRes];
			didInit = true;
			offerCleanupTimer = setInterval(
				() =>
					(offerPool = offerPool.filter((peer) => {
						const shouldLive = Date.now() - peer.created < offerTtl;

						if (!shouldLive) {
							peer.destroy();
						}

						return shouldLive;
					})),
				offerTtl * 1.03
			);
			cleanupWatchOnline = config.manualRelayReconnection ? noOp : watchOnline();
		}

		const announceIntervals = initPromises.map(() => announceIntervalMs);
		const announceTimeouts: ReturnType<typeof setTimeout>[] = [];

		const unsubFns = initPromises.map(async (relayP, i) =>
			subscribe(await relayP, await rootTopicP, await selfTopicP, handleMessage(i), getOffers)
		);

		all([rootTopicP, selfTopicP]).then(([rootTopic, selfTopic]) => {
			const queueAnnounce = async (relay: unknown, i: number) => {
				const ms = await announce(relay, rootTopic, selfTopic);

				if (typeof ms === 'number') {
					announceIntervals[i] = ms;
				}

				announceTimeouts[i] = setTimeout(() => queueAnnounce(relay, i), announceIntervals[i]);
			};

			unsubFns.forEach(async (didSub, i) => {
				await didSub;
				queueAnnounce(await initPromises[i], i);
			});
		});

		let onPeerConnect = noOp;

		occupiedRooms[appId] ||= {};

		return (occupiedRooms[appId][roomId] = room(
			(f) => (onPeerConnect = f),
			(id) => delete connectedPeers[id],
			() => {
				delete occupiedRooms[appId][roomId];
				announceTimeouts.forEach(clearTimeout);
				unsubFns.forEach(async (f) => (await f)());
				clearInterval(offerCleanupTimer);
				cleanupWatchOnline();
				didInit = false;
			}
		));
	};
};
