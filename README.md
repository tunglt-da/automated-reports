Warehouse Automation

This project automates data extraction from Redash, Power BI report generation, and report distribution for warehouse tasks.

Features

1. Fetch data from Redash and save to CSV.
2. Refresh Power BI reports and export data/images.
3. Send reports via webhooks.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/warehouse-automation.git
   cd warehouse-automation
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:

   - Create a `.env` file in `src/` with the following:

     ```
     REDASH_DOMAIN=https://your-redash-domain.com
     WEBHOOK_URL=https://your-webhook-url
     SERVICE_ACCOUNT_PATH=/path/to/your/service_account.json
     SHEET_ID=your-google-sheet-id
     DATA_PATH=/path/to/data
     LOG_PATH=/path/to/logs
     PBI_TITLE=your-powerbi-title
     ```

4. Set up Google API credentials:

   - Place your service account JSON file at the path specified in `SERVICE_ACCOUNT_PATH`.

## Usage

Run the scheduler:

```bash
python src/main.py set_schedule
```

Run a one-time manual task:

```bash
python src/main.py run_manual_once
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.
