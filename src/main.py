import logging
import os
import sys
from configparser import SectionProxy, ConfigParser
from typing import List, Union

import pandas as pd
from telegram.ext import ApplicationBuilder, Application

from twitterForwardBot.feed import grab_and_publish, send_heart_beat


def load_config_from_ini(sysargv: List[str], section_name: str) -> SectionProxy:
    working_dir = sysargv[1]
    config_file_name = sysargv[2]
    config_ini_file = os.path.join(working_dir, config_file_name)
    config_parser = ConfigParser()
    if not os.path.isfile(config_ini_file):
        raise RuntimeError(f"cannot find {config_ini_file}")
    config_parser.read(config_ini_file)
    config = config_parser[section_name]
    return config


def init_application_context(
    application: Application, config: Union[dict, SectionProxy], sysargv: List[str]
) -> Application:
    application.bot_data["heart_beat_chat"] = int(config["heart_beat_chat"])
    application.bot_data["publish_interval"] = 3  ## seconds
    application.bot_data["publish_chat"] = int(config["publish_chat"])
    application.bot_data["time_back"] = pd.Timedelta("2 hours")
    application.bot_data["ids"] = ["mtrainier2020", "starzqeth"]
    return application


def init_application_handler(application: Application) -> Application:
    application.job_queue.run_repeating(grab_and_publish, first=5, interval=60 * 60)
    application.job_queue.run_repeating(send_heart_beat, first=5, interval=4 * 60 * 60)
    return application


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    logger = logging.getLogger(__name__)

    config = load_config_from_ini(sys.argv, "twitter_forward_bot")
    application = (
        ApplicationBuilder()
        .token(config["token"])
        .http_version("1.1")
        .get_updates_http_version("1.1")
        .concurrent_updates(3)
        .build()
    )
    application = init_application_context(application, config, sys.argv)
    application = init_application_handler(application)
    application.run_polling()
