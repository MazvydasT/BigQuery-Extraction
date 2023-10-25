import { BigQueryDate, BigQueryTimestamp } from '@google-cloud/bigquery';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { from } from 'ix/iterable';
import { map as ixMap } from 'ix/iterable/operators';
import jsonexport from 'jsonexport';
import moment from 'moment';
import { firstValueFrom, map, mergeMap, retry, timer, toArray } from 'rxjs';
import { utils, writeXLSX } from 'xlsx';
import { AppModule } from './app.module';
import { BigQueryService } from './big-query/big-query.service';
import { AllowedExtensions, ConfigurationService } from './configuration/configuration.service';
import { OutputService } from './output/output.service';
import { getAdditionalProperties } from './utils';

async function bootstrap() {
	//const app = await NestFactory.create(AppModule);
	const app = await NestFactory.createApplicationContext(AppModule);
	const configurationService = app.get(ConfigurationService);
	const bigQueryService = app.get(BigQueryService);
	const outputService = app.get(OutputService);

	const logger = new Logger(`main`);

	const retryConfig = {
		count: configurationService.retries,
		delay: configurationService.retryDelay,
		resetOnSuccess: true
	};

	while (true) {
		logger.log(`Starting extraction`);

		try {
			const outputExtension = configurationService.outputExtension;

			let orderedFieldNames: string[] | undefined = undefined;

			const outputRows = await firstValueFrom(
				bigQueryService.read<any>().pipe(
					map(({ chunk, schema }) => {
						const newChunk = Object.fromEntries(
							from(Object.entries(chunk)).pipe(
								ixMap(([key, value]) => {
									let newValue = value;

									try {
										if (newValue instanceof BigQueryDate)
											newValue = moment(newValue.value).format(`DD/MM/YYYY`);
										else if (newValue instanceof BigQueryTimestamp)
											newValue = moment(newValue.value).format(`DD/MM/YYYY HH:mm:ss`);
									} catch (error) {
										throw Object.assign(new Error(error.message), {
											...getAdditionalProperties(error),
											key,
											value,
											...chunk
										});
									}

									return [key, newValue];
								}) //,

								//orderBy(([key]) => key)
							)
						);

						return { chunk: newChunk, schema };
					}),

					mergeMap(async ({ chunk, schema }, index) => {
						if (index == 0) {
							orderedFieldNames = schema?.fields?.map(field => field.name ?? ``) ?? [];

							if (!configurationService.doNotSortColumnsAlphabetically) {
								orderedFieldNames?.sort();
							}
						}

						switch (outputExtension) {
							case AllowedExtensions.csv:
								return `${index == 0 ? '\ufeff' : ''}${await jsonexport([chunk], {
									includeHeaders: index == 0,
									headers: orderedFieldNames ?? []
								})}\n`;

							case AllowedExtensions.xlsx:
								return chunk;
						}
					}),

					retry(retryConfig),

					toArray()
				),
				{ defaultValue: new Array<any>() }
			);

			if (outputRows.length > 0) {
				let outputData: string | Buffer;

				switch (outputExtension) {
					case AllowedExtensions.csv:
						outputData = outputRows.join(``);
						break;

					case AllowedExtensions.xlsx:
						const sheet = utils.json_to_sheet(outputRows, {
							cellDates: true,
							header: orderedFieldNames ?? []
						});
						const workbook = utils.book_new();
						utils.book_append_sheet(workbook, sheet, `Data`);

						outputData = writeXLSX(workbook, { compression: true, type: `buffer` });
						break;
				}

				let outputFilename = configurationService.output;
				const outputExtensionAsString = AllowedExtensions[outputExtension];

				if (!!configurationService.timestampFormat) {
					const outputFilenameWithoutExtension = outputFilename.substring(
						0,
						outputFilename.length - (outputExtensionAsString.length + 1)
					);

					outputFilename =
						outputFilenameWithoutExtension +
						moment().format(configurationService.timestampFormat) +
						`.${outputExtensionAsString}`;
				}

				await firstValueFrom(
					outputService.outputToFile(outputData, outputFilename).pipe(retry(retryConfig))
				);
			}
		} catch (error) {
			logger.error(error, ...getAdditionalProperties(error), error.stack);

			// Persistent error cooldown
			logger.log(
				`Persistent error occured. Will retry in ${moment
					.duration(configurationService.persistentErrorCooldown)
					.humanize()}.`
			);

			await firstValueFrom(timer(configurationService.persistentErrorCooldown));

			continue;
		}

		logger.log(`Extraction completed`);

		const cron = configurationService.cron;

		if (!cron) break;

		const now = moment();

		cron.reset(now.toDate());

		const nextExtractionStart = moment(cron.next().value.toDate());
		const msToStartAnotherExtraction = Math.max(nextExtractionStart.diff(now), 0);

		logger.log(
			`Next extraction will start in ${moment
				.duration(msToStartAnotherExtraction)
				.humanize()} ${nextExtractionStart.calendar({
				sameDay: `[today at] HH:mm`,
				nextDay: `[tomorrow at] HH:mm`,
				nextWeek: `[on] dddd [at] HH:mm`
			})}`
		);

		await firstValueFrom(timer(msToStartAnotherExtraction));
	}

	//await app.listen(3000);
}

bootstrap();
