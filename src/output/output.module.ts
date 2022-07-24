import { Module } from '@nestjs/common';
import { FileModule } from '../file/file.module';
import { OutputService } from './output.service';

@Module({
	imports: [FileModule],
	providers: [OutputService],
	exports: [OutputService]
})
export class OutputModule {}
