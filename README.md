# fully-automated-dialer-list-generation


This repository contains a fully automated R pipeline for generating dialer-ready contact lists on a scheduled basis.

The project is designed to run unattended, transform raw input data into a clean dialer list, and deliver the output automatically via email or file export.

It is built to be reproducible, debuggable, and safe to operate in production environments.


# What This Project Does

Reads input data from predefined sources

Applies deterministic filtering and transformation logic

Generates a dialer-compatible output file

Sends the output automatically via email

Runs on a schedule without manual intervention


Project Structure
.
├── run_daily.R              # Main entry point for scheduled execution
├── config.example.yml       # Configuration template (no secrets)
├── R/
│   ├── data_prep.R          # Data loading and preprocessing
│   ├── transformation.R    # Business logic and filtering rules
│   └── utils.R              # Helper functions
├── logs/                    # Runtime logs (ignored by git)
├── output/                  # Generated dialer lists (ignored by git)
├── .gitignore
└── README.md




All environment-specific values are defined in a YAML configuration file.

The repository includes a config.example.yml file that documents the required structure.


# How It Runs

The pipeline is executed via:

Rscript run_daily.R


In production, this command is intended to be triggered by the operating system scheduler (e.g. Windows Task Scheduler or cron).

The script exits cleanly on success and fails loudly on configuration or data errors.

Output is only sent if all steps complete successfully

This prevents partial or corrupted dialer lists from being distributed.


# Requirements

R (tested on recent 4.x versions)

Required R packages listed inside the scripts

SMTP access for email delivery if enabled

This repository is intended to demonstrate production-grade automation rather than exploratory analysis.

# License

Internal or portfolio use. Adapt freely.
