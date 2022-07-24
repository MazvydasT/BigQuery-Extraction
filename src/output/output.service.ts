import { Injectable } from '@nestjs/common';
import { Readable } from 'stream';
import { FileService } from '../file/file.service';

@Injectable()
export class OutputService {
	constructor(private fileService: FileService) {}

	/*private itemsArrayToReadable(itemsIterator: Iterator<any>) {
		return new Readable({
			read() {
				const result = itemsIterator.next();

				this.push(result.done ? null : `${JSON.stringify(result.value)}\n`);
			}
		});
	}*/

	private stringToReadable(data: string) {
		return new Readable({
			read() {
				this.push(data);
				this.push(null);
			}
		});
	}

	/*outputToFile(items: IterableX<any>, path: string) {
		return this.fileService.write(this.itemsArrayToReadable(items[Symbol.iterator]()), path);
	}*/

	outputToFile(data: string, path: string) {
		return this.fileService.write(this.stringToReadable(data), path);
	}
}
