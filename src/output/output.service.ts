import { Injectable } from '@nestjs/common';
import { Readable } from 'stream';
import { FileService } from '../file/file.service';

@Injectable()
export class OutputService {
	constructor(private fileService: FileService) {}

	private stringToReadable(data: string | Buffer) {
		return new Readable({
			read() {
				this.push(data);
				this.push(null);
			}
		});
	}

	outputToFile(data: string | Buffer, path: string) {
		return this.fileService.write(this.stringToReadable(data), path);
	}
}
