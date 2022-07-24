import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { BigQueryModule } from './big-query/big-query.module';
import { ConfigurationModule } from './configuration/configuration.module';
import { OutputModule } from './output/output.module';

@Module({
	imports: [ConfigurationModule, BigQueryModule, OutputModule],
	controllers: [AppController],
	providers: [AppService]
})
export class AppModule {}
