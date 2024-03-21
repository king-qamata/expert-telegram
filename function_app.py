import azure.functions as func
import snowflake.connector
import aiohttp
import asyncio
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import logging


app = func.FunctionApp()

@app.schedule(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)

async def main(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    # Define a semaphore to limit the number of concurrent requests
    semaphore = asyncio.Semaphore(80)  # Adjust the value as needed

    def get_connection():
        return snowflake.connector.connect(
            account='kuiugta-xx34639',
            user='LSJHoward',
            password='Titan100!',
            warehouse='SOCIAL_MEDIA_API_LOADER'
        )

    def get_personal_accounts():
        connection = get_connection()
        database = "ACTIVE_AUDIT__DEV_DB"
        schema = "SEEDS"
        seed = "SEED_ACTIVE_AUDIT__PERSONAL_ACCOUNTS"

        if database and schema and seed and connection:
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT DISTINCT public_url FROM {}.{}.{}".format(database, schema, seed))
                results = cursor.fetchall()  # Fetch all rows from the result set
                return results
            except Exception as e:
                logging.info("Failed to return a result from Snowflake:", e)
            finally:
                cursor.close()
                connection.close()
        else:
            logging.info("Not all required variables are set.")
            return []

    async def get_linkedin_profile(session, public_id):
        url = "https://fresh-linkedin-profile-data.p.rapidapi.com/get-linkedin-profile"
        querystring = {"linkedin_url": f"https://www.linkedin.com/in/{public_id}/", "include_skills": "true"}
        headers = {
            "X-RapidAPI-Key": "3d86fb27b0mshedabb6152fbce3ap1a8767jsn48d1f221405f",
            "X-RapidAPI-Host": "fresh-linkedin-profile-data.p.rapidapi.com"
        }
        async with semaphore:
            async with session.get(url, headers=headers, params=querystring) as response:
                if response.status == 200:
                    return await response.json(), dict(response.headers)
                else:
                    logging.info(f"Failed to retrieve LinkedIn profile for {public_id}: Status code {response.status}")
                    return None, None

    async def get_linkedin_posts(session, public_id):
        url = "https://fresh-linkedin-profile-data.p.rapidapi.com/get-profile-posts"
        querystring = {"linkedin_url": f"https://www.linkedin.com/in/{public_id}/", "type": "posts"}
        headers = {
            "X-RapidAPI-Key": "3d86fb27b0mshedabb6152fbce3ap1a8767jsn48d1f221405f",
            "X-RapidAPI-Host": "fresh-linkedin-profile-data.p.rapidapi.com"
        }
        async with semaphore:
            async with session.get(url, headers=headers, params=querystring) as response:
                if response.status == 200:
                    return await response.json(), dict(response.headers)
                else:
                    logging.info(f"Failed to retrieve LinkedIn posts for {public_id}: Status code {response.status}")
                    return None, None

    async def upload_to_azure_blob(container_name, filename, file_content):
        try:
            # Retrieve the connection string for your storage account
            connect_str = 

            # Create a BlobServiceClient object
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)

            # Create the container client for the specified container
            container_client = blob_service_client.get_container_client(container_name)

            # Upload the JSON file to Azure Blob Storage
            blob_client = container_client.get_blob_client(filename)
            blob_client.upload_blob(file_content)

            logging.info(f"Uploaded {filename} to Azure Blob Storage container: {container_name}")

        except Exception as e:
            logging.info(f"Error uploading {filename} to Azure Blob Storage container {container_name}: {e}")

    personal_accounts = get_personal_accounts()
    if personal_accounts:  # Check if the list is not empty
        async with aiohttp.ClientSession() as session:
            tasks = []
            for account in personal_accounts:
                public_id = account[0]  # Extract the public ID from the tuple
                # Get LinkedIn profile data for the public_id
                profile_task = get_linkedin_profile(session, public_id)
                # Get LinkedIn posts data for the public_id
                posts_task = get_linkedin_posts(session, public_id)
                tasks.extend([profile_task, posts_task])
            results = await asyncio.gather(*tasks)
            for (profile_data, profile_headers), (posts_data, posts_headers), account in zip(results[::2], results[1::2], personal_accounts):
                public_id = account[0]  # Extract the public ID from the tuple
                if profile_data and posts_data:
                    # Get the current date and time
                    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    
                    # Upload profile data to Azure Blob Storage
                    upload_profile_task = upload_to_azure_blob("personal-profile-body", f"{current_datetime}_{public_id}_profile.json", json.dumps({public_id: profile_data}))
                    await asyncio.create_task(upload_profile_task)
                    
                    # Upload profile response headers to Azure Blob Storage
                    upload_profile_headers_task = upload_to_azure_blob("personal-profile-headers", f"{current_datetime}_{public_id}_profile_headers.json", json.dumps({public_id: profile_headers}))
                    await asyncio.create_task(upload_profile_headers_task)
                    
                    # Upload posts data to Azure Blob Storage
                    upload_posts_task = upload_to_azure_blob("personal-posts-body", f"{current_datetime}_{public_id}_posts.json", json.dumps({public_id: posts_data}))
                    await asyncio.create_task(upload_posts_task)
                    
                    # Upload posts response headers to Azure Blob Storage
                    upload_posts_headers_task = upload_to_azure_blob("personal-posts-headers", f"{current_datetime}_{public_id}_posts_headers.json", json.dumps({public_id: posts_headers}))
                    await asyncio.create_task(upload_posts_headers_task)

                    # Upload all four files to cold storage
                    all_files_content = {
                        f"{current_datetime}_{public_id}_profile.json": json.dumps({public_id: profile_data}),
                        f"{current_datetime}_{public_id}_profile_headers.json": json.dumps({public_id: profile_headers}),
                        f"{current_datetime}_{public_id}_posts.json": json.dumps({public_id: posts_data}),
                        f"{current_datetime}_{public_id}_posts_headers.json": json.dumps({public_id: posts_headers})
                    }
                    upload_all_files_task = upload_to_azure_blob("personal-cold-storage", f"{current_datetime}_{public_id}_all_files.json", json.dumps(all_files_content))
                    await asyncio.create_task(upload_all_files_task)
    else:
        logging.info("No personal accounts found.")