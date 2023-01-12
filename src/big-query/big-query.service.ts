import { BigQuery, GetRowsOptions } from '@google-cloud/bigquery';
import { Injectable } from '@nestjs/common';
import { Observable } from 'rxjs';
import { ConfigurationService } from '../configuration/configuration.service';

@Injectable()
export class BigQueryService {
	constructor(private configurationService: ConfigurationService) {}

	private bigQuery = new BigQuery({
		projectId: this.configurationService.bigQueryProject,
		keyFilename: this.configurationService.bigQueryKeyFilename
	});

	private bigQueryDataset = this.bigQuery.dataset(this.configurationService.bigQueryDataset);

	read<T>(options?: GetRowsOptions) {
		return new Observable<T>(subscriber => {
			const readStream = !!this.configurationService.bigQueryTable
				? this.bigQueryDataset
						.table(this.configurationService.bigQueryTable)
						.createReadStream(options)
				: this.bigQueryDataset.createQueryStream(this.configurationService.sql ?? ``);

			readStream
				.on(`error`, err => subscriber.error(Object.assign(new Error(err.message), err)))
				.on(`data`, chunk => subscriber.next(chunk))
				.on(`close`, () => subscriber.complete());

			return () => readStream.destroy();
		});
	}
}
