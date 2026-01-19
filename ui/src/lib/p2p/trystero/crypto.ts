// @ts-nocheck

import { decodeBytes, encodeBytes } from './utils';

const algo = 'AES-GCM';
const strToSha1: Record<string, string> = {};

const pack = (buff: ArrayBuffer) => {
	const bytes = new Uint8Array(buff);
	let binary = '';

	for (let i = 0; i < bytes.length; i++) {
		binary += String.fromCharCode(bytes[i]);
	}

	return btoa(binary);
};

const unpack = (packed: string) => {
	const str = atob(packed);
	return new Uint8Array(str.length).map((_, i) => str.charCodeAt(i)).buffer;
};

export const hashWith = async (algorithm: AlgorithmIdentifier, str: string) =>
	new Uint8Array(await crypto.subtle.digest(algorithm, encodeBytes(str)));

export const sha1 = async (str: string) =>
	(strToSha1[str] ||= Array.from(await hashWith('SHA-1', str))
		.map((b) => b.toString(36))
		.join(''));

export const genKey = async (secret: string, appId: string, roomId: string) =>
	crypto.subtle.importKey(
		'raw',
		await crypto.subtle.digest({ name: 'SHA-256' }, encodeBytes(`${secret}:${appId}:${roomId}`)),
		{ name: algo },
		false,
		['encrypt', 'decrypt']
	);

const joinChar = '$';
const ivJoinChar = ',';

export const encrypt = async (keyP: PromiseLike<CryptoKey> | CryptoKey, plaintext: string) => {
	const iv = crypto.getRandomValues(new Uint8Array(16));

	return (
		iv.join(ivJoinChar) +
		joinChar +
		pack(await crypto.subtle.encrypt({ name: algo, iv }, await keyP, encodeBytes(plaintext)))
	);
};

export const decrypt = async (keyP: PromiseLike<CryptoKey> | CryptoKey, raw: string) => {
	const [iv, c] = raw.split(joinChar);

	return decodeBytes(
		await crypto.subtle.decrypt(
			{ name: algo, iv: new Uint8Array(iv.split(ivJoinChar)) },
			await keyP,
			unpack(c)
		)
	);
};
