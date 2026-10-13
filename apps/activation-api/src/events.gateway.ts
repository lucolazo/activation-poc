import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server } from 'socket.io';

/** Envía cada cambio de una activación a la UI (RF-08). */
@WebSocketGateway({ cors: { origin: '*' } })
export class EventsGateway {
  @WebSocketServer() server: Server;
  broadcast(activation: any) { this.server?.emit('activation.update', activation); }
}
