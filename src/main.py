import sys
import asyncio
import pandas as pd
import requests
import logging
import time
import json
import aiohttp
from tenacity import retry, stop_after_attempt, wait_fixed
from tabulate import tabulate
import gspread
from google.oauth2 import service_account
import schedule
import ctypes
import pyautogui
import pyperclip
from pywinauto import Application, Desktop
from .utils import setup_logging, capture_area, upload_image, convert_numpy
from .config import CONFIG

# Redash Exception
class RedashDataException(Exception):
    def __init__(self, row, original_exception):
        super().__init__(str(original_exception))
        self.row = row
        self.original_exception = original_exception

# Redash Functions
@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
async def get_redash_data(row, session, failed_info_dict, start_run):
    """Fetch data from a Redash query and save to CSV."""
    row['execute_status'] = 'failed'
    row['query_result_id'] = 0
    row['query_result'] = pd.DataFrame()
    row['rows_cnt'] = 0
    row['runtime'] = 0
    row['execute_time'] = 0

    redash_headers = {'Authorization': f"Key {row['api_key']}"}
    params = json.loads(row['params'].replace("'", "\"")) if row['params'] else {}
    start_execute = time.time()
    executing = False
    in_queue = False

    try:
        url_result = f"{CONFIG['redash_domain']}/api/queries/{row['query_id']}/results"
        payload_result = {
            'apply_auto_limit': False,
            'id': row['query_id'],
            'max_age': 0,
            'parameters': params
        }
        async with session.post(url_result, json=payload_result, headers=redash_headers) as response:
            response_result_json = await response.json()
            if response.status != 200:
                if 'job' in response_result_json:
                    raise Exception(response_result_json['job'].get('error', response_result_json))
                else:
                    raise Exception(response_result_json.get('message', response_result_json))

        job_id = response_result_json['job'].get('id', None)
        if job_id is None:
            raise Exception(response_result_json['job']['error'])
        
        url_job = f"{CONFIG['redash_domain']}/api/jobs/{job_id}"
        while True:
            async with session.get(url_job, headers=redash_headers) as request_job:
                job_result = await request_job.json()
                job_status = job_result['job']['status']
                
                if job_status == 1:
                    if not in_queue:
                        logging.info(f"Query in queue...: {row['query_id']} - {row['query_name']}")
                    in_queue = True
                
                elif job_status == 2:
                    if not executing:
                        logging.info(f"Executing query...: {row['query_id']} - {row['query_name']}")
                        start_execute = time.time()
                    in_queue = False
                    executing = True

                elif job_status == 3:
                    logging.info(f"Completed query {row['query_id']} - {row['query_name']}")
                    query_result_id = job_result['job']['query_result_id']
                    url_query_result_id = f"{CONFIG['redash_domain']}/api/query_results/{query_result_id}"
                    async with session.get(url_query_result_id, headers=redash_headers) as request_query_result:
                        response_query_result = await request_query_result.json()
                        execute_time = "{:.2f}".format(response_query_result['query_result']['runtime'])
                        if response_query_result['query_result']['data']['rows']:
                            query_result = pd.DataFrame(response_query_result['query_result']['data']['rows'])
                        else:
                            columns = [col['name'] for col in response_query_result['query_result']['data']['columns']]
                            rows = [[
                                time.strftime('%Y-%m-%d %H:%M') if col['name'] == 'datetime_run'
                                else params['wh_id'] if col['name'] == 'wh_hub_id' and 'wh_id' in params
                                else '' for col in response_query_result['query_result']['data']['columns']]]
                            query_result = pd.DataFrame(rows, columns=columns)
                        query_result.to_csv(os.path.join(CONFIG['data_path'], f"{row['query_save_name']}.csv"), index=False)

                    row['execute_status'] = 'success'
                    row['query_result_id'] = query_result_id
                    row['query_result'] = query_result
                    row['rows_cnt'] = len(query_result)
                    row['runtime'] = "{:.2f}".format(time.time() - start_run)
                    row['execute_time'] = execute_time
                    return row

                elif job_status == 4 or job_status == 5:
                    raise Exception(job_result['job']['error'])

    except Exception as error:
        ref_name = f"{row['query_id']}{row['query_name']}"
        failed_info_dict[ref_name].append({
            'query_id': row['query_id'], 
            'query_name': row['query_name'], 
            'attempt': len(failed_info_dict[ref_name]) + 1, 
            'error': str(error)
        })
        webhook_msg = f"Attempt {len(failed_info_dict[ref_name])} failed: Query {row['query_id']} - {row['query_name']}: {error}"
        logging.error(webhook_msg)
        row['runtime'] = "{:.2f}".format(time.time() - start_run)
        raise RedashDataException(row, error)

async def execute_redash(task_list):
    """Execute Redash queries for the given task list."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskQueries").get_all_records())
    task_queries = sheet[(sheet['active_flag'].str.lower() == 'y') & (sheet['run_flag'].str.lower() == 'y') & (sheet['task_name'].isin(task_list))].reset_index(drop=True)
    task_queries = task_queries.drop_duplicates(subset=['query_id', 'params', 'query_save_name'], keep='first')
    logging.info(f'Task queries:\n{task_queries}')

    failed_info_dict = {f"{row['query_id']}{row['query_name']}": [] for row in task_queries.to_dict('records')}
    async with aiohttp.ClientSession() as session:
        start_run = time.time()
        tasks = [get_redash_data(row, session, failed_info_dict, start_run) for row in task_queries.to_dict('records')]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    final_results = []
    for result in results:
        if isinstance(result, RetryError):
            last_exception = result.last_attempt.exception()
            if isinstance(last_exception, RedashDataException):
                row = last_exception.row
                row['failed_info'] = failed_info_dict.get(f"{row['query_id']}{row['query_name']}", [])
                final_results.append(row)
        elif isinstance(result, RedashDataException):
            row = result.row
            row['failed_info'] = failed_info_dict.get(f"{row['query_id']}{row['query_name']}", [])
            final_results.append(row)
        else:
            result['failed_info'] = failed_info_dict.get(f"{result['query_id']}{result['query_name']}", [])
            final_results.append(result)

    failed_details = [info for row in final_results if 'failed_info' in row for info in row['failed_info']]
    if failed_details:
        sorted_failed_details = sorted(failed_details, key=lambda x: (x['query_id'], x['query_name'], x['attempt']))
        requests.post(CONFIG['webhook_err'], json={'text': "Redash failed data:\n```{}```".format(tabulate(sorted_failed_details, headers='keys'))})

    fields_to_extract = ['query_id', 'query_name', 'execute_status', 'runtime', 'execute_time', 'rows_cnt', 'query_result_id']
    extracted_data = [{field: row[field] for field in fields_to_extract if field in row} for row in final_results]
    requests.post(CONFIG['webhook_err'], json={'text': "Redash Results:\n```{}```".format(tabulate(extracted_data, headers='keys'))})

# Power BI Functions
def get_pbi_window():
    """Get the Power BI window by title."""
    SW_SHOWMAXIMIZED = 3
    try:
        windows = Desktop(backend="uia").windows(title_re=".*" + CONFIG['pbi_title'] + ".*")
        if windows:
            window_id = windows[0].handle
            ctypes.windll.user32.ShowWindow(window_id, SW_SHOWMAXIMIZED)
            pbi = Application(backend="uia").connect(handle=window_id).window(handle=window_id)
            pbi.set_focus()
            time.sleep(3)
            return pbi
        else:
            logging.error(f"No window found with title: {CONFIG['pbi_title']}")
            return None
    except Exception as e:
        logging.error(f"Could not find or connect to PBI window: {e}")
        return None

def refresh_pbi(pbi):
    """Refresh the Power BI dataset."""
    try:
        pbi.child_window(title="Home", control_type="TabItem", found_index=0).click_input()
        time.sleep(1)
        pbi.child_window(title="Refresh", control_type="Button", found_index=0).click_input()
        time.sleep(1)

        start_time = time.time()
        while time.time() - start_time < 600:
            try:
                close_button = pbi.child_window(title="Close", control_type="Button", found_index=1)
                if close_button.exists() and close_button.is_visible():
                    time.sleep(15)
                    close_button.click_input()
                    time.sleep(3)
                    return True
            except:
                time.sleep(5)
        cancel_button = pbi.child_window(title="Cancel", control_type="Button", found_index=1)
        if cancel_button.exists() and cancel_button.is_visible():
            cancel_button.click_input()
            return False
        return True
    except Exception as e:
        logging.error(f"Error refreshing PBI: {e}")
        return False

def select_pbi_page(page_name, sleeper):
    """Select a specific page in Power BI."""
    pbi = get_pbi_window()
    if pbi:
        try:
            logging.info(f'Attempting to click on page: {page_name}')
            tab_item = pbi.child_window(title=page_name, control_type="TabItem", found_index=0)
            if tab_item.exists():
                tab_item.wrapper_object().set_focus()
                if tab_item.is_enabled() and tab_item.is_visible():
                    tab_item.click_input()
                else:
                    tab_item.invoke()
                logging.info(f"Clicked on page: {page_name}")
                time.sleep(int(sleeper) if sleeper else 3)
            else:
                logging.error(f"TabItem '{page_name}' not found.")
        except Exception as e:
            logging.error(f"Failed to interact with page: {e}")
    else:
        logging.error("PBI window is None.")

def export_pbi_file(table_title, sleeper, export_path):
    """Export a table from Power BI to a file."""
    pbi = get_pbi_window()
    if pbi:
        try:
            logging.info(f'Clicking table: {table_title}')
            pbi.child_window(title_re=table_title, control_type='Group', found_index=0).click_input()
            time.sleep(1)
            pbi.child_window(title="More options", control_type="Button", found_index=0).click_input()
            time.sleep(1)
            pbi.child_window(title="Export data", control_type="MenuItem", found_index=0).click_input()
            time.sleep(int(sleeper) if sleeper else 3)

            start_time = time.time()
            save_as_window = None
            while time.time() - start_time < 30:
                try:
                    save_as_window = pbi.child_window(title="Save As", control_type="Window")
                    if save_as_window.exists() and save_as_window.is_visible():
                        logging.info('Save As window loaded successfully')
                        break
                except Exception as e:
                    logging.info(f'Waiting for Save As window... {e}')
                time.sleep(1)
            
            if save_as_window is None or not save_as_window.exists():
                logging.error('Save As window did not load')
                return

            pyperclip.copy(export_path)
            time.sleep(0.5)
            pyautogui.hotkey('ctrl', 'v')
            time.sleep(1)
            pyautogui.hotkey('Enter')
            time.sleep(2)

            start_time = time.time()
            while time.time() - start_time < 15:
                try:
                    confirm_window = pbi.child_window(title="Confirm Save As", control_type="Window")
                    if confirm_window.exists() and confirm_window.is_visible():
                        confirm_window.child_window(title="Yes", auto_id="CommandButton_6", control_type="Button").click_input()
                        break
                except:
                    pass
                time.sleep(1)
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error exporting PBI file: {e}")

# Report Functions
def send_report(task_list):
    """Send reports based on task configurations."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    
    try:
        sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskMsg").get_all_records())
        tasks = sheet[(sheet['proceed_flag'].str.lower() == 'y') & (sheet['task_name'].isin(task_list))].reset_index(drop=True)

        for task in tasks.to_dict('records'):
            select_pbi_page(page_name=task['page'], sleeper=task['page_sleep'])
            if task['report'] == 'image':
                image_path = capture_area(left=task['left'], top=task['top'], right=task['right'], bottom=task['bottom'], export_name=task['export_name'])
                if image_path is None:
                    raise Exception('Export image failed')
                image_link = upload_image(file_path=image_path, folder_id=task['folder'], service_account_path=CONFIG['service_account_path'])
                if image_link is None:
                    raise Exception('Upload image failed')
                if task['send_flag'].lower() == 'y':
                    if task['hyperlink']:
                        requests.post(task['webhook'], json={"text": f"{task['msg_content']}\n{image_link}\nFor detailed data, click <{task['hyperlink']}|*here*>"})
                    else:
                        requests.post(task['webhook'], json={'text': f"{task['msg_content']}\n{image_link}"})
            
            elif task['report'] == 'file':
                export_pbi_file(table_title=task['table_title'], sleeper=task['exp_sleep'], export_path=os.path.join(task['folder'], task['export_name']))
                if task['send_flag'].lower() == 'y':
                    requests.post(task['webhook'], json={'text': f"{task['msg_content']}\n{task['hyperlink']}"})
    
    except Exception as e:
        logging.error(f"Send report error: {str(e)}")

# Scheduler Functions
def set_schedule():
    """Set up the task schedule from Google Sheets."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskSchedule").get_all_records())
    for time_slot in sheet.columns[4:]:
        task_list = sheet[sheet[time_slot].str.lower() == 'x']['task_name'].tolist()
        if task_list:
            schedule.every().day.at(time_slot).do(main, get_redash=True, ref_pbi=True, task_list=task_list)
    
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            current_time = time.strftime("%H:%M:%Analyzer
            logging.error(f"Schedule failed at {current_time}. Error: {e}")
            time.sleep(1)

def run_manual_once():
    """Run tasks marked for one-time execution."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskSchedule").get_all_records())
    task_list = sheet[sheet['once'].str.lower() == 'x']['task_name'].tolist()
    if task_list:
        main(get_redash=True, ref_pbi=True, task_list=task_list)

def run_manual_quick():
    """Run tasks marked for quick execution."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskSchedule").get_all_records())
    task_list = sheet[sheet['quick'].str.lower() == 'x']['task_name'].tolist()
    if task_list:
        main(ref_pbi=True, task_list=task_list)

# Main Function
def main(get_redash=False, ref_pbi=False, task_list=[]):
    """Main function to orchestrate task execution."""
    setup_logging(CONFIG['log_path'])
    logging.info('Started send report: {}'.format(", ".join(task for task in task_list)))
    try:
        if get_redash:
            asyncio.run(execute_redash(task_list))
            logging.info('Redash execution finished. Wait 15s to sync files.')
            time.sleep(15)
        
        logging.info('Going to refresh PBI')
        pbi = get_pbi_window()
        if ref_pbi:
            if not refresh_pbi(pbi):
                requests.post(CONFIG['webhook_err'], json={'text': "PBI refresh failed, cancelled"})
                raise Exception('PBI refresh failed, cancelled')

        logging.info('Sending reports')
        send_report(task_list)
        logging.info('Ended send report')
    except Exception as e:
        requests.post(CONFIG['webhook_err'], json={'text': f'Function main failed: {str(e)}'})
        logging.error(f'Function main failed: {str(e)}')
        logging.info('Ended send report')

if __name__ == "__main__":
    if len(sys.argv) > 1:
        function_name = sys.argv[1]
        if function_name == "set_schedule":
            set_schedule()
        elif function_name == "run_manual_once":
            run_manual_once()
        elif function_name == "run_manual_quick":
            run_manual_quick()
        else:
            print(f"Function '{function_name}' not found.")
    else:
        print("No function name provided.")
