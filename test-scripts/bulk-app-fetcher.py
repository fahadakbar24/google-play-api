from gpapi.googleplay import GooglePlayAPI, RequestError
from pprint import pprint
from tqdm import tqdm
from pathlib import Path
from yachalk import chalk
import pymongo
import configparser

config = None
server = None


# Fetch downloads collection
def get_downloads_list():
    mongo_client = pymongo.MongoClient(config["database"]["db_url"])
    mongo_db = mongo_client[config["database"]["db_name"]]
    return mongo_db['apkDownloads']


# Server Setup
def init_server():
    server = GooglePlayAPI(
        config["locale"]["language"],
        config["locale"]["timezone"]
    )

    print("\nLogging in ...")

    server.login(None, None, int(config["credentials"]["gsf_id"]), config["credentials"]["auth_sub_token"])
    # server.login(config["credentials"]["gmail_address"], config["credentials"]["gmail_password"])
    # pprint(server.__dict__)

    return server


# DOWNLOAD
def download_apk(doc_id, storage_path):
    dwnld_resp = server.download(doc_id)

    file_storage_path = f"{storage_path}{doc_id}.apk"
    with open(file_storage_path, "wb") as apk_file, tqdm(
            desc=doc_id, total=int(dwnld_resp.get('file').get('total_size')), unit='iB', unit_scale=True, unit_divisor=1024,
    ) as bar:
        for chunk in dwnld_resp.get("file").get("data"):
            wrtn_size = apk_file.write(chunk)
            bar.update(wrtn_size)


# collect apks
def collect_apks_for_category(dwnld_collection, cat_id, sub_cat):
    global ttl_size
    ttl_size = 0
    cur_downloads = 0
    next_list = None
    dwnld_lmt = int(config["settings"]["cat_download_limit"])
    max_apk_size = int(config["settings"]["max_apk_size"])
    storage_location = config["settings"]["storage_location"]
    storage_path = f"{storage_location}/{cat_id}_"

    #  create separate category folder if not exists
    Path(storage_location).mkdir(parents=True, exist_ok=True)

    while cur_downloads < dwnld_lmt:
        app_list, next_list = server.cluster_list(cat_id, sub_cat, next_list)

        if len(app_list) < 1:
            break

        # print("Downloading ({} to {}) of {} apks from '{}'...".format(cur_downloads, cur_downloads + len(app_list),
        # dwnld_lmt, cat_id))

        for app in app_list:
            app_details = {
                "category_id": cat_id,
                "sub_category": sub_cat,
                "apk_docid": app['docid'],
                "size": int(app['details']['appDetails']['file'][0]['size'])
            }

            ttl_size += app_details['size']

            if dwnld_collection.find_one(app_details):
                cur_downloads += 1
                # print(chalk.red(f"Skipping '{app['docid']}'... APP ALREADY DOWNLOADED."))
                continue
            elif app_details['size'] > max_apk_size:
                # print(chalk.red(f"Skipping '{app['docid']}'... SIZE IS NOT BEARABLE."))
                continue
            else:
                print(chalk.red(f"Downloading {cur_downloads+1} of {dwnld_lmt}"))
                download_apk(app['docid'], storage_path)
                # # os.system(""" cmd /c "gplaycli -d \"{}\" -p -f \"./play_store_apks/\" " """.format(doc_id))
                cur_downloads += 1
                dwnld_collection.insert_one(app_details)

        if not next_list:  # Stop downloading if there are no apps available
            break

    return cur_downloads


# bulk_apk_download
def cat_apk_bulk_download(dwnld_collection):
    total_downloads = 0
    dwnld_lmt = int(config["settings"]["cat_download_limit"])
    sub_cats = {
        'TOP_FREE': 'topselling_free',
        'TOP_PAID': 'topselling_paid',
        'GROSSING': 'topgrossing',
        'TRENDING': 'movers_shakers',
        'TOP_FREE_GAMES': 'topselling_free_games',
        'TOP_PAID_GAMES': 'topselling_paid_games',
        'TOP_GROSSING_GAMES': 'topselling_grossing_games',
        'NEW_FREE': 'topselling_new_free',
        'NEW_PAID': 'topselling_new_paid',
        'NEW_FREE_GAMES': 'topselling_new_free_games',
        'NEW_PAID_GAMES': 'topselling_new_paid_games'
    }

    print("\nStarting the download process ...")
    browse = server.browse()

    for loop_idx, cat in enumerate(browse.get("category")):
        cat_id = cat['dataUrl'][cat['dataUrl'].find('cat=')+4:cat['dataUrl'].find('&c=')]
        print("\n\nDownloading {} apks from {}-'{}' under '{}'".format(dwnld_lmt, loop_idx, cat_id, sub_cats['TOP_FREE']))

        if '?docid=' in cat_id or dwnld_collection.count_documents({"category_id": cat_id}) > dwnld_lmt:
            print(chalk.bg_white.red_bright("Skipping '{}' due to invalid id or the download limit reached".format(cat_id)))
            continue

        total_downloads += collect_apks_for_category(dwnld_collection, cat_id, sub_cats['TOP_FREE'])

    print(f"Total Downloads: {total_downloads}")
    print(f"Total Size: {ttl_size}")


def main():
    global config
    global server

    # Reading stored configurations
    config = configparser.ConfigParser()
    config.read("gplaycli.conf")

    print(chalk.bg_white.green("\nStarting server ..."))
    server = init_server()

    cat_apk_bulk_download(get_downloads_list())


main()
