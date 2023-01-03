"""Console script for notion_taskwarrior."""
from functools import partial
import os
import sys
from typing import List

import click
from bubop import (
    check_optional_mutually_exclusive,
    format_dict,
    log_to_syslog,
    logger,
    loguru_tqdm_sink,
)

from taskwarrior_syncall import inform_about_app_extras

try:
    from taskwarrior_syncall import NotionDBSide
except ImportError:
    inform_about_app_extras(["notion"])


# from notion_client import Client  # type: ignore
from notional import connect

from taskwarrior_syncall import (
    Aggregator,
    TaskWarriorCustomSide,
    __version__,
    cache_or_reuse_cached_combination,
    fetch_app_configuration,
    fetch_from_pass_manager,
    get_resolution_strategy,
    inform_about_combination_name_usage,
    list_named_combinations,
    opt_combination,
    opt_custom_combination_savename,
    opt_list_combinations,
    opt_notion_token_pass_path,
    opt_resolution_strategy,
    report_toplevel_exception,
    convert_custom_tw_to_notion_db,
    convert_notion_db_to_custom_tw
    
)


# CLI parsing ---------------------------------------------------------------------------------
@click.command()
# Notion options ------------------------------------------------------------------------------
# @opt_notion_page_id()
@click.argument("todo_db_id", type=str)
@click.argument("project_db_id", type=str)
@opt_notion_token_pass_path()
# misc options --------------------------------------------------------------------------------
@opt_resolution_strategy()
@opt_combination("TWCustom", "NotionDB")
@opt_list_combinations("TWCustom", "NotionDB")
@opt_custom_combination_savename("TWCustom", "NotionDB")
@click.option("-v", "--verbose", count=True)
@click.version_option(__version__)
def main(
    todo_db_id: str,
    project_db_id: str,
    token_pass_path: str,
    resolution_strategy: str,
    verbose: int,
    combination_name: str,
    custom_combination_savename: str,
    do_list_combinations: bool,
):
    """Synchronise filters of TW tasks with the to_do items of Notion pages

    The list of TW tasks is determined by a combination of TW tags and TW project while the
    notion pages should be provided by their URLs.
    """
    # setup logger ----------------------------------------------------------------------------
    loguru_tqdm_sink(verbosity=verbose)
    log_to_syslog(name="tw_notion_sync")
    logger.debug("Initialising...")
    inform_about_config = False
    if do_list_combinations:
        list_named_combinations(config_fname="tw_notion_configs")
        return 0

    # cli validation --------------------------------------------------------------------------
    check_optional_mutually_exclusive(combination_name, custom_combination_savename)
    combination_of_tw_project_tags_and_notion_page = any(
        [
            # tw_project,
            # tw_tags,
            todo_db_id,

        ]
    )
    check_optional_mutually_exclusive(
        combination_name, combination_of_tw_project_tags_and_notion_page
    )

    # existing combination name is provided ---------------------------------------------------
    if combination_name is not None:
        app_config = fetch_app_configuration(
            config_fname="tw_notion_configs", combination=combination_name
        )
        # tw_tags = app_config["tw_tags"]
        # tw_project = app_config["tw_project"]
        todo_db_id = app_config["todo_db_id"]

    # combination manually specified ----------------------------------------------------------
    else:
        inform_about_config = True
        combination_name = cache_or_reuse_cached_combination(
            config_args={
                "todo_db_id": todo_db_id,
                "project_db_id": project_db_id,
                # "tw_project": tw_project,
                # "tw_tags": tw_tags,
            },
            config_fname="tw_notion_configs",
            custom_combination_savename=custom_combination_savename,
        )

    # announce configuration ------------------------------------------------------------------
    logger.info(
        format_dict(
            header="Configuration",
            items={
                "Notion TODO db ID": todo_db_id,
                "Notion Projects db ID": project_db_id,
            },
            prefix="\n\n",
            suffix="\n",
        )
    )

    # find token to connect to notion ---------------------------------------------------------
    token_v2 = os.environ.get("NOTION_API_KEY")
    if token_v2 is not None:
        logger.debug("Reading the Notion API key from environment variable...")
    else:
        token_v2 = fetch_from_pass_manager(token_pass_path)

    assert token_v2

    # initialize taskwarrior ------------------------------------------------------------------
    tw_side = TaskWarriorCustomSide(sync_value="notion")

    # initialize notion -----------------------------------------------------------------------
    client = connect(auth=token_v2)
    notion_side = NotionDBSide(client=client, todo_db_id=todo_db_id, project_db_id=project_db_id)
    project_id_to_short_name = {page.id : page.properties["ShortName"].Value
                                for page in client.databases.query(project_db_id).execute()
                                if page.properties["ShortName"].Value != ""}
    convert_custom_tw_to_notion_db_partial = partial(convert_custom_tw_to_notion_db,
                                                     project_id_to_short_name=project_id_to_short_name)
    convert_notion_db_to_custom_tw_partial = partial(convert_notion_db_to_custom_tw,
                                                     project_id_to_short_name=project_id_to_short_name)
    


    # sync ------------------------------------------------------------------------------------
    try:
        with Aggregator(
            side_A=notion_side,
            side_B=tw_side,
            converter_B_to_A=convert_custom_tw_to_notion_db_partial,
            converter_A_to_B=convert_notion_db_to_custom_tw_partial,
            resolution_strategy=get_resolution_strategy(
                resolution_strategy, side_A_type=type(notion_side), side_B_type=type(tw_side)
            ),
            config_fname=combination_name,
            ignore_keys=(
                ("last_modified_date",),
                ("due", "end", "entry", "modified", "urgency"),
            ),
        ) as aggregator:
            aggregator.sync()
    except KeyboardInterrupt:
        logger.error("Exiting...")
        return 1
    except:
        report_toplevel_exception(is_verbose=verbose >= 1)
        return 1

    if inform_about_config:
        inform_about_combination_name_usage(combination_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
