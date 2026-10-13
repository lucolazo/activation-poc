import { Body, Controller, Get, HttpCode, NotFoundException, Param, Post } from '@nestjs/common';
import { ActivationsService } from './activations.service';

@Controller('activations')
export class ActivationsController {
  constructor(private readonly service: ActivationsService) {}

  @Post() @HttpCode(202) // RF-02: no espera a los demás servicios
  create(@Body() dto: any) { return this.service.create(dto); }

  @Get(':id') // RF-09: estado actual + historial
  async get(@Param('id') id: string) {
    const doc = await this.service.get(id);
    if (!doc) throw new NotFoundException(`Activación ${id} no existe`);
    return doc;
  }
}
