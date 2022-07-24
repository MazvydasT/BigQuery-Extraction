import { BigQueryDate, BigQueryTimestamp } from '@google-cloud/bigquery';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import jsonexport from 'jsonexport';
import moment from 'moment';
import { firstValueFrom, map, mergeMap, retry, timer, toArray } from 'rxjs';
import { AppModule } from './app.module';
import { BigQueryService } from './big-query/big-query.service';
import { ConfigurationService } from './configuration/configuration.service';
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
			const outputRows = await firstValueFrom(
				bigQueryService.read<any>().pipe(
					map(chunk => {
						const newChunk = Object.fromEntries(
							Object.entries(chunk).map(([key, value]) => {
								let newValue = value;

								if (newValue instanceof BigQueryDate)
									newValue = moment(newValue.value).format(`DD/MM/YYYY`);
								else if (newValue instanceof BigQueryTimestamp)
									newValue = moment(newValue.value).format(`DD/MM/YYYY HH:mm:ss`);

								return [key, newValue];
							})
						);

						return newChunk;
					}),

					mergeMap(async (chunk, index) => {
						return `${index == 0 ? '\ufeff' : ''}${await jsonexport([chunk], {
							includeHeaders: index == 0
						})}\n`;
					}),

					retry(retryConfig),

					toArray()
				),
				{ defaultValue: new Array<string>() }
			);

			if (outputRows.length > 0) {
				const outputData = outputRows.join(``);

				await firstValueFrom(
					outputService
						.outputToFile(outputData, configurationService.output)
						.pipe(retry(retryConfig))
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
