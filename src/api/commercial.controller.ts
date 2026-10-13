import { Controller, Get, NotFoundException, Param } from '@nestjs/common';
import { CommercialRepository } from '../persistence/commercial.repository';
import { SyncStateRepository } from '../persistence/sync-state.repository';

@Controller()
export class CommercialController {
  constructor(
    private readonly commercial: CommercialRepository,
    private readonly syncState: SyncStateRepository,
  ) {}

  @Get('affiliates/:id')
  async getAffiliate(@Param('id') id: string) {
    const affiliate = await this.commercial.getAffiliate(id);
    if (!affiliate) throw new NotFoundException();
    const [deals, sync] = await Promise.all([
      this.commercial.listDealsByAffiliate(id),
      this.syncState.get('affiliate', id),
    ]);
    return { ...affiliate, sync, deals };
  }

  @Get('deals/:id')
  async getDeal(@Param('id') id: string) {
    const deal = await this.commercial.getDeal(id);
    if (!deal) throw new NotFoundException();
    return { ...deal, sync: await this.syncState.get('deal', id) };
  }
}
