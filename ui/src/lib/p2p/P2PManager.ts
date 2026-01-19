import { joinRoom, selfId } from '$lib/p2p/trystero';
import { getSearchParam, setSearchParam } from '$lib/utils/search-params';

export type P2PMessageHandler = (data: unknown, peerId: string) => void;
export type P2PConnectionState = 'disconnected' | 'connecting' | 'connected';

type TrysteroSend = (data: unknown, targets?: string | string[]) => Promise<void>;
type TrysteroRoom = {
	makeAction: (
		type: string
	) => [
		TrysteroSend,
		(onComplete: (data: unknown, peerId: string, meta?: unknown) => void) => void,
		(onProgress: (progress: number, peerId: string, meta?: unknown) => void) => void
	];
	getPeers: () => Record<string, unknown>;
	onPeerJoin: (handler: (peerId: string) => void) => void;
	onPeerLeave: (handler: (peerId: string) => void) => void;
	leave: () => Promise<void> | void;
};

type ChannelAction = {
	send: TrysteroSend;
	handlers: Set<P2PMessageHandler>;
};

const APP_ID = 'patchies-trystero';

export class P2PManager {
	private static instance: P2PManager | null = null;
	private room: TrysteroRoom | null = null;
	private channels = new Map<string, ChannelAction>();
	private roomId: string = '';
	private initializePromise: Promise<void> | null = null;
	private connectionState: P2PConnectionState = 'disconnected';
	private peerCount = 0;

	private constructor() {
		const room = getSearchParam('room');

		if (room) {
			this.roomId = room;
		} else {
			this.roomId = crypto.randomUUID();
			setSearchParam('room', this.roomId);
		}
	}

	public static getInstance(): P2PManager {
		if (!P2PManager.instance) {
			P2PManager.instance = new P2PManager();
		}

		return P2PManager.instance;
	}

	public async initialize(): Promise<void> {
		if (this.room) return;
		if (this.initializePromise) return this.initializePromise;

		this.connectionState = 'connecting';
		this.initializePromise = this._initialize();

		return this.initializePromise;
	}

	private async _initialize(): Promise<void> {
		this.room = joinRoom({ appId: APP_ID }, this.roomId) as unknown as TrysteroRoom;
		this.connectionState = 'connected';
		this.room.onPeerJoin?.(() => this.updatePeerCount());
		this.room.onPeerLeave?.(() => this.updatePeerCount());
		this.updatePeerCount();
	}

	private ensureRoom(): asserts this is { room: TrysteroRoom } {
		if (!this.room) {
			throw new Error('[p2p] P2P manager not initialized');
		}
	}

	private getOrCreateChannel(channel: string): ChannelAction {
		const existing = this.channels.get(channel);
		if (existing) return existing;

		this.ensureRoom();
		const [send, setOnComplete] = this.room.makeAction(channel);
		const handlers = new Set<P2PMessageHandler>();

		setOnComplete((data, peerId) => {
			handlers.forEach((handler) => {
				try {
					handler(data, peerId);
				} catch (error) {
					console.error('[p2p] Error handling message:', error);
				}
			});
		});

		const action = { send, handlers };
		this.channels.set(channel, action);
		return action;
	}

	public subscribeToChannel(channel: string, handler: P2PMessageHandler): () => void {
		const action = this.getOrCreateChannel(channel);
		action.handlers.add(handler);

		return () => {
			action.handlers.delete(handler);
		};
	}

	public sendToChannel(channel: string, data: unknown): void {
		try {
			const action = this.getOrCreateChannel(channel);
			void action.send(data);
		} catch (error) {
			console.error('[p2p] Error sending message:', error);
		}
	}

	public getPeerCount(): number {
		return this.peerCount;
	}

	private updatePeerCount(): void {
		if (!this.room) {
			this.peerCount = 0;
			return;
		}

		this.peerCount = Object.keys(this.room.getPeers?.() ?? {}).length;
	}

	public getConnectionState(): P2PConnectionState {
		return this.connectionState;
	}

	public isConnected(): boolean {
		return this.connectionState === 'connected';
	}

	public getMyPeerId(): string {
		return selfId;
	}

	public getRoomId(): string {
		return this.roomId;
	}

	public destroy(): void {
		this.room?.leave();
		this.room = null;
		this.channels.clear();
		this.connectionState = 'disconnected';
		this.initializePromise = null;
		this.peerCount = 0;
		P2PManager.instance = null;
	}
}
