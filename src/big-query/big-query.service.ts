import { BigQuery, GetRowsOptions } from '@google-cloud/bigquery';
import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { ConfigurationService } from '../configuration/configuration.service';

@Injectable()
export class BigQueryService {
	constructor(private configurationService: ConfigurationService) {}

	private bigQueryTable = new BigQuery({
		projectId: this.configurationService.bigQueryProject,
		keyFilename: this.configurationService.bigQueryKeyFilename
	})
		.dataset(this.configurationService.bigQueryDataset)
		.table(this.configurationService.bigQueryTable);

	read<T>(options?: GetRowsOptions) {
		return new Observable<T>(subscriber => {
			const readStream = this.bigQueryTable
				.createReadStream(options)
				.on(`error`, err => subscriber.error(Object.assign(new Error(err.message), err)))
				.on(`data`, chunk => subscriber.next(chunk))
				.on(`close`, () => subscriber.complete());

			return () => readStream.destroy();
		});
	}
}
