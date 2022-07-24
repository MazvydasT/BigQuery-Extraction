import { Injectable } from '@nestjs/common';
import { createWriteStream } from 'fs';
import { Observable } from 'rxjs';
import { Readable } from 'stream';

@Injectable()
export class FileService {
	write(readable: Readable, path: string) {
		return new Observable(subscriber => {
			try {
				const writeStream = createWriteStream(path);

				readable
					.pipe(writeStream)
					.on(`close`, () => {
						subscriber.next(true);
						subscriber.complete();
					})
					.on(`error`, err => subscriber.error(Object.assign(new Error(err.message), err)));
			} catch (error) {
				subscriber.error(Object.assign(new Error(error.message), error));
			}
		});
	}
}
