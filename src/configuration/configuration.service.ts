import { Injectable } from '@nestjs/common';
import { Command, InvalidArgumentError, Option } from 'commander';
import { CronExpression, parseExpression } from 'cron-parser';
import { config } from 'dotenv';
import { parseIntClamp } from '../utils';

@Injectable()
export class ConfigurationService {
	private readonly optionValues = (() => {
		const envOption = new Option(`--env <path>`, `Path to .env file`).env(`ENV`);

		const envPath = new Command().addOption(envOption).parse().opts<{ env?: string }>().env;

		config({ path: envPath, override: true });

		const bqtableOption = `--bqtable`;
		const sqlOption = `--sql`;

		const command = new Command()
			.addOption(envOption)

			.addOption(
				new Option(`-o, --output <path>`, `Output file path`)
					.env(`OUTPUT`)
					.makeOptionMandatory(true)
			)

			.addOption(
				new Option(`-r, --retry <count>`, `Retry errors`)
					.env(`RETRY`)
					.default(5)
					.argParser(value => {
						try {
							return parseIntClamp(value, { min: 0 });
						} catch (_) {
							throw new InvalidArgumentError(``);
						}
					})
			)
			.addOption(
				new Option(`--retry-delay <ms>`, `Time delay in ms before retrying errors`)
					.env(`RETRY_DELAY`)
					.default(10000)
					.argParser(value => {
						try {
							return parseIntClamp(value, { min: 0 });
						} catch (_) {
							throw new InvalidArgumentError(``);
						}
					})
			)

			.addOption(
				new Option(
					`-c, --persistent-error-cooldown <ms>`,
					`Time in ms between re-extarction attempts after persistent error`
				)
					.env(`PERSISTENT_ERROR_COOLDOWN`)
					.default(600000)
					.argParser(parseInt)
			)

			.addOption(
				new Option(`--cron <expression>`, `Cron expression to schedule extraction`)
					.env(`CRON`)
					.argParser(value => {
						try {
							return !value ? undefined : parseExpression(value, { iterator: true });
						} catch (_) {
							throw new InvalidArgumentError(``);
						}
					})
			)

			.addOption(
				new Option(`--bqkeyfile <filepath>`, 'BigQuery key file')
					.env(`BQKEYFILE`)
					.makeOptionMandatory(true)
			)
			.addOption(
				new Option(`--bqproject <name>`, `BigQuery project name`)
					.env(`BQPROJECT`)
					.makeOptionMandatory(true)
			)
			.addOption(
				new Option(`--bqdataset <name>`, `BigQuery dataset name`)
					.env(`BQDATASET`)
					.makeOptionMandatory(true)
			)
			.addOption(new Option(`${bqtableOption} <name>`, `BigQuery table name`).env(`BQTABLE`))

			.addOption(
				new Option(`${sqlOption} <query>`, `Custom SQL query instead of a table name`).env(`SQL`)
			)

			.showHelpAfterError(true)

			.parse();

		const options = command.opts<{
			output: string;

			retry: number;
			retryDelay: number;

			persistentErrorCooldown: number;

			cron?: CronExpression<true>;

			bqkeyfile: string;
			bqproject: string;
			bqdataset: string;
			bqtable?: string;

			sql?: string;
		}>();

		if (!options.bqtable && !options.sql)
			command.error(`${bqtableOption} or ${sqlOption} must be set`);

		return Object.freeze(options);
	})();

	get output() {
		return this.optionValues.output;
	}

	get retries() {
		return this.optionValues.retry;
	}
	get retryDelay() {
		return this.optionValues.retryDelay;
	}

	get persistentErrorCooldown() {
		return this.optionValues.persistentErrorCooldown;
	}

	get cron() {
		return this.optionValues.cron;
	}

	get bigQueryKeyFilename() {
		return this.optionValues.bqkeyfile;
	}
	get bigQueryProject() {
		return this.optionValues.bqproject;
	}
	get bigQueryDataset() {
		return this.optionValues.bqdataset;
	}
	get bigQueryTable() {
		return this.optionValues.bqtable;
	}

	get sql() {
		return this.optionValues.sql;
	}
}
