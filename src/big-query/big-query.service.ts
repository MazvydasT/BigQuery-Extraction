import {
	BigQuery,
	GetRowsOptions,
	JobMetadata,
	TableMetadata,
	TableSchema
} from '@google-cloud/bigquery';
import { ResourceStream } from '@google-cloud/paginator';
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
		return new Observable<{ chunk: T; schema: TableSchema | undefined }>(subscriber => {
			const readStreamPromise = (async () => {
				let readStream: ResourceStream<any>;
				let tableMetadata: TableMetadata | null = null;

				if (!!this.configurationService.bigQueryTable) {
					const table = this.bigQueryDataset.table(this.configurationService.bigQueryTable);

					tableMetadata = await table
						.getMetadata()
						.then(metadataResponse => metadataResponse[0] as TableMetadata);

					readStream = table.createReadStream(options);
				} else {
					const [queryJob] = await this.bigQueryDataset.createQueryJob(
						this.configurationService.sql ?? ``
					);

					const jobMetadata = await queryJob
						.getMetadata()
						.then(jobResponse => jobResponse[0] as JobMetadata);

					const destinationTable = jobMetadata.configuration?.query?.destinationTable;

					if (!!destinationTable && !!destinationTable.datasetId && !!destinationTable.tableId) {
						tableMetadata = await this.bigQuery
							.dataset(destinationTable.datasetId)
							.table(destinationTable.tableId)
							.getMetadata()
							.then(metadataResponse => metadataResponse[0] as TableMetadata);
					}

					readStream = queryJob.getQueryResultsStream();
				}

				const schema = tableMetadata?.schema;

				readStream
					.on(`error`, err => subscriber.error(Object.assign(new Error(err.message), err)))
					.on(`data`, chunk => subscriber.next({ chunk, schema }))
					.on(`close`, () => subscriber.complete());

				return readStream;
			})().catch(reason => {
				subscriber.error(Object.assign(new Error(reason.message), reason));
				return null;
			});

			return () => readStreamPromise.then(readStream => readStream?.destroy());
		});
	}
}
