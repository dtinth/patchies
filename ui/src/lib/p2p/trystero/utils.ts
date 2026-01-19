// @ts-nocheck

const { floor, random, sin } = Math;

export const libName = 'Trystero';

export const alloc = (n: number, f: (value: unknown, index: number, array: unknown[]) => unknown) =>
	Array(n).fill(undefined).map(f);

const charSet = '0123456789AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz';

export const genId = (n: number) =>
	alloc(n, () => charSet[floor(random() * charSet.length)]).join('');

export const selfId = genId(20);

export const all = Promise.all.bind(Promise);

export const isBrowser = typeof window !== 'undefined';

export const { entries, fromEntries, keys } = Object;

export const noOp = () => {};

export const mkErr = (msg: string) => new Error(`${libName}: ${msg}`);

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const encodeBytes = (txt: string) => encoder.encode(txt);

export const decodeBytes = (buffer: ArrayBuffer | ArrayLike<number>) => decoder.decode(buffer);

export const toHex = (buffer: ArrayLike<number>) =>
	buffer.reduce((a, c) => a + c.toString(16).padStart(2, '0'), '');

export const topicPath = (...parts: string[]) => parts.join('@');

export const shuffle = <T>(xs: T[], seed: number) => {
	const a = [...xs];
	const rand = () => {
		const x = sin(seed++) * 10_000;
		return x - floor(x);
	};

	let i = a.length;

	while (i) {
		const j = floor(rand() * i--);
		[a[i], a[j]] = [a[j], a[i]];
	}

	return a;
};

export const getRelays = (
	config: { relayUrls?: string[]; relayRedundancy?: number; appId: string },
	defaults: string[],
	defaultN: number,
	deriveFromAppId?: boolean
) => {
	const relayUrls =
		config.relayUrls || (deriveFromAppId ? shuffle(defaults, strToNum(config.appId)) : defaults);

	return relayUrls.slice(
		0,
		config.relayUrls ? config.relayUrls.length : config.relayRedundancy || defaultN
	);
};

export const toJson = JSON.stringify;

export const fromJson = JSON.parse;

export const strToNum = (str: string, limit = Number.MAX_SAFE_INTEGER) =>
	str.split('').reduce((a, c) => a + c.charCodeAt(0), 0) % limit;

const defaultRetryMs = 3333;
const socketRetryPeriods: Record<string, number> = {};

let reconnectionLockingPromise: Promise<void> | null = null;
let resolver: (() => void) | null = null;

export const pauseRelayReconnection = () => {
	if (!reconnectionLockingPromise) {
		reconnectionLockingPromise = new Promise<void>((resolve) => {
			resolver = resolve;
		}).finally(() => {
			resolver = null;
			reconnectionLockingPromise = null;
		});
	}
};

export const resumeRelayReconnection = () => resolver?.();

export const makeSocket = (url: string, onMessage: (data: string) => void) => {
	const client: {
		socket?: WebSocket;
		url?: string;
		ready?: Promise<unknown>;
		send?: (data: string) => void;
	} = {};

	const init = () => {
		const socket = new WebSocket(url);
		socket.onclose = () => {
			if (reconnectionLockingPromise) {
				reconnectionLockingPromise.then(init);
				return;
			}
			socketRetryPeriods[url] ??= defaultRetryMs;
			setTimeout(init, socketRetryPeriods[url]);
			socketRetryPeriods[url] *= 2;
		};

		socket.onmessage = (e) => onMessage(e.data as string);
		client.socket = socket;
		client.url = socket.url;
		client.ready = new Promise((res) => {
			socket.onopen = () => {
				res(client);
				socketRetryPeriods[url] = defaultRetryMs;
			};
		});
		client.send = (data) => {
			if (socket.readyState === 1) {
				socket.send(data);
			}
		};
	};

	init();

	return client;
};

export const socketGetter = (clientMap: Record<string, { socket: WebSocket }>) => () =>
	fromEntries(entries(clientMap).map(([url, client]) => [url, client.socket]));

export const watchOnline = () => {
	if (isBrowser) {
		const controller = new AbortController();

		addEventListener('online', resumeRelayReconnection, {
			signal: controller.signal
		});
		addEventListener('offline', pauseRelayReconnection, {
			signal: controller.signal
		});

		return () => controller.abort();
	}

	return noOp;
};
