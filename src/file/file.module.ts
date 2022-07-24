import { Module } from '@nestjs/common';
import { ConfigurationModule } from '../configuration/configuration.module';
import { FileService } from './file.service';

@Module({
	imports: [ConfigurationModule],
	providers: [FileService],
	exports: [FileService]
})
export class FileModule {}
