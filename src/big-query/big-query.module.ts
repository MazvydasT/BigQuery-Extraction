import { Module } from '@nestjs/common';
import { ConfigurationModule } from 'src/configuration/configuration.module';
import { BigQueryService } from './big-query.service';

@Module({
	imports: [ConfigurationModule],
	providers: [BigQueryService],
	exports: [BigQueryService]
})
export class BigQueryModule {}
