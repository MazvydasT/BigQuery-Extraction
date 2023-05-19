import { Injectable } from '@nestjs/common';
import { Command, InvalidArgumentError, Option } from 'commander';
import { CronExpression, parseExpression } from 'cron-parser';
import { config } from 'dotenv';
import { extname } from 'path';
import { parseIntClamp } from '../utils';

export enum AllowedExtensions {
	csv,
	xlsx
}

@Injectable()
export class ConfigurationService {
	private selectedExtension: AllowedExtensions;

	private readonly optionValues = (() => {
		const envOption = new Option(`--env <path>`, `Path to .env file`).env(`ENV`);

		const envPath = new Command().addOption(envOption).parse().opts<{ env?: string }>().env;

		config({ path: envPath, override: true });

		const bqtableOption = `--bqtable`;
		const sqlOption = `--sql`;

		const allowedExtensions = Object.values(AllowedExtensions).filter(
			value => typeof value == 'string'
		);
		const allowedExtensionAsString = allowedExtensions.join(', ');

		const command = new Command()
			.addOption(envOption)

			.addOption(
				new Option(
					`-o, --output <path>`,
					`Output file path. Allowed extensions: ${allowedExtensionAsString}.`
				)
					.env(`OUTPUT`)
					.makeOptionMandatory(true)
					.argParser(value => {
						const extension = extname(value);
						const extensionWithoutADot = extension.substring(1).toLowerCase();

						const allowedExtensionIndex = allowedExtensions.indexOf(extensionWithoutADot);

						if (allowedExtensionIndex > -1) {
							this.selectedExtension = allowedExtensionIndex;
							return value;
						}

						throw new InvalidArgumentError(
							`Unsupported output extension ${extensionWithoutADot}. Allowed extensions: ${allowedExtensionAsString}.`
						);
					})
			)

			.addOption(
				new Option(`-t, --timestamp-format <format>`, `Timestamp to append to output filename`).env(
					`TIMESTAMP_FORMAT`
				)
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

			timestampFormat?: string;

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

	get outputExtension() {
		return this.selectedExtension;
	}

	get timestampFormat() {
		return this.optionValues.timestampFormat;
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
