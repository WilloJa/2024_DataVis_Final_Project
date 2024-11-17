import time
from steam.webapi import WebAPI
import os
import json
import csv
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import deque

#### INITIAL PARAMETERS ####

API_KEY = "23C2E43F9847BF436F138154FF7EDDC6"
api = WebAPI(key=API_KEY)
data_dir = "Steam_Scraped_Data"
os.makedirs(data_dir, exist_ok=True)

error_log_file = os.path.join(data_dir, "error_log.txt")
output_csv = os.path.join(data_dir, "all_steam_data.csv")
output_json = os.path.join(data_dir, "all_steam_data.json")

successful_file = os.path.join(data_dir, "successful_ids.txt")
error_401_file = os.path.join(data_dir, "error_401_ids.txt")
retry_file = os.path.join(data_dir, "retry_ids.txt")

starting_steam_id = "76561197979408421"  # Kongzoola
max_depth = 3
max_threads = 6
batch_size = 10
api_call_count = 0
api_limit = 100000  # Daily API call limit
reset_time = datetime.now() + timedelta(days=1)

#### FUNCTIONS ####

def log_error(message):
    with open(error_log_file, 'a') as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")

def load_existing_ids(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def append_to_file(file_path, steam_id):
    with open(file_path, 'a') as f:
        f.write(f"{steam_id}\n")

def make_api_call(func, max_retries=3, delay=5, **kwargs):
    global api_call_count, reset_time
    api_call_count += 1

    if api_call_count > api_limit:
        if datetime.now() >= reset_time:
            api_call_count = 0
            reset_time = datetime.now() + timedelta(days=1)
        else:
            print("API limit reached. Pausing until reset...")
            time_until_reset = (reset_time - datetime.now()).total_seconds()
            time.sleep(time_until_reset)
            api_call_count = 0

    for attempt in range(max_retries):
        try:
            return func(**kwargs)
        except Exception as e:
            if "429" in str(e):
                time.sleep(delay * (2 ** attempt))
            else:
                log_error(f"API call failed: {e}")
                break
    log_error(f"Max retries exceeded for API call.")
    return None

def calculate_friendship_duration(friend_since):
    friend_since_date = datetime.fromtimestamp(friend_since)
    duration = datetime.now() - friend_since_date
    return {
        "friend_since": friend_since_date.strftime("%Y-%m-%d"),
        "friendship_duration_days": duration.days
    }

def get_player_summaries_in_batches(steam_ids):
    summaries = []
    batch_size = 100
    for i in range(0, len(steam_ids), batch_size):
        batch = steam_ids[i:i + batch_size]
        try:
            summary_data = make_api_call(api.ISteamUser.GetPlayerSummaries, steamids=",".join(batch))
            if summary_data:
                summaries.extend(summary_data.get("response", {}).get("players", []))
        except Exception as e:
            log_error(f"Error in player summary batch {i // batch_size + 1}: {e}")
    return summaries

def collect_player_data(steam_id):
    global successful_ids, error_401_ids, retry_ids

    if steam_id in successful_ids:
        return None  # Skip already successful IDs

    player_data = {"steam_id": steam_id, "data": {}}
    try:
        player_summaries = get_player_summaries_in_batches([steam_id])
        if player_summaries:
            player_data["data"]["player_info"] = player_summaries[0]

        owned_games = make_api_call(
            api.IPlayerService.GetOwnedGames,
            steamid=steam_id, include_appinfo=True,
            include_played_free_games=True,
            appids_filter=[]
        )
        if owned_games:
            player_data["data"]["owned_games"] = owned_games

        friends = make_api_call(api.ISteamUser.GetFriendList, steamid=steam_id)
        if friends and 'friendslist' in friends:
            friends_data = []
            for friend in friends['friendslist']['friends']:
                friend_id = friend['steamid']
                friend_since = friend.get('friend_since')
                if friend_since:
                    friends_data.append({
                        "friend_id": friend_id,
                        **calculate_friendship_duration(friend_since)
                    })
            player_data["data"]["friends"] = friends_data

        append_to_file(successful_file, steam_id)
        successful_ids.add(steam_id)
    except Exception as e:
        error_message = str(e)
        if "401" in error_message:
            append_to_file(error_401_file, steam_id)
            error_401_ids.add(steam_id)
        else:
            append_to_file(retry_file, steam_id)
            retry_ids.add(steam_id)
        log_error(f"Error processing Steam ID {steam_id}: {e}")

    return player_data

def gather_steam_ids(steam_id, max_depth):
    visited_ids = set([steam_id])
    queue = deque([(steam_id, 0)])  # Store (steam_id, current_depth)

    while queue:
        current_id, depth = queue.popleft()
        if depth >= max_depth:
            continue

        try:
            friends = make_api_call(api.ISteamUser.GetFriendList, steamid=current_id)
            if friends and 'friendslist' in friends:
                for friend in friends['friendslist']['friends']:
                    friend_id = friend['steamid']
                    if friend_id not in visited_ids:
                        visited_ids.add(friend_id)
                        queue.append((friend_id, depth + 1))
        except Exception as e:
            log_error(f"Error retrieving friends for {current_id}: {e}")

    return visited_ids

def process_players_in_batches(steam_ids):
    global batch_counter, starting_steam_id
    player_batch = []
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {executor.submit(collect_player_data, steam_id): steam_id for steam_id in steam_ids}

        with tqdm(total=len(steam_ids), desc="Processing players", unit="player", dynamic_ncols=True) as pbar:
            for future in as_completed(futures):
                steam_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        player_batch.append(result)
                except Exception as e:
                    log_error(f"Error processing Steam ID {steam_id}: {e}")
                pbar.update(1)

    with open(output_json, 'w') as f:
        json.dump({"players": player_batch}, f, indent=4)

#### MAIN SCRIPT ####

successful_ids = load_existing_ids(successful_file)
error_401_ids = load_existing_ids(error_401_file)
retry_ids = load_existing_ids(retry_file)

all_steam_ids = gather_steam_ids(starting_steam_id, max_depth)
new_steam_ids = list(all_steam_ids - successful_ids - error_401_ids - retry_ids)

process_players_in_batches(new_steam_ids)