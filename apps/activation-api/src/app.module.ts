import { Module } from '@nestjs/common';
import { ActivationsController } from './activations.controller';
import { ActivationsService } from './activations.service';
import { EventsGateway } from './events.gateway';

@Module({ controllers: [ActivationsController], providers: [ActivationsService, EventsGateway] })
export class AppModule {}
