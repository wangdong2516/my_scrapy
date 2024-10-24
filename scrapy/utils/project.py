from __future__ import annotations

import os
import warnings
from importlib import import_module
from pathlib import Path

from scrapy.exceptions import NotConfigured
from scrapy.settings import Settings
from scrapy.utils.conf import closest_scrapy_cfg, get_config, init_env

ENVVAR = "SCRAPY_SETTINGS_MODULE"
DATADIR_CFG_SECTION = "datadir"


def inside_project() -> bool:
    scrapy_module = os.environ.get(ENVVAR)
    if scrapy_module:
        try:
            import_module(scrapy_module)
        except ImportError as exc:
            warnings.warn(
                f"Cannot import scrapy settings module {scrapy_module}: {exc}"
            )
        else:
            return True
    return bool(closest_scrapy_cfg())


def project_data_dir(project: str = "default") -> str:
    """Return the current project data dir, creating it if it doesn't exist"""
    if not inside_project():
        raise NotConfigured("Not inside a project")
    cfg = get_config()
    if cfg.has_option(DATADIR_CFG_SECTION, project):
        d = Path(cfg.get(DATADIR_CFG_SECTION, project))
    else:
        scrapy_cfg = closest_scrapy_cfg()
        if not scrapy_cfg:
            raise NotConfigured(
                "Unable to find scrapy.cfg file to infer project data dir"
            )
        d = (Path(scrapy_cfg).parent / ".scrapy").resolve()
    if not d.exists():
        d.mkdir(parents=True)
    return str(d)


def data_path(path: str | os.PathLike[str], createdir: bool = False) -> str:
    """
    Return the given path joined with the .scrapy data directory.
    If given an absolute path, return it unmodified.
    """
    path_obj = Path(path)
    if not path_obj.is_absolute():
        if inside_project():
            path_obj = Path(project_data_dir(), path)
        else:
            path_obj = Path(".scrapy", path)
    if createdir and not path_obj.exists():
        path_obj.mkdir(parents=True)
    return str(path_obj)


def get_project_settings() -> Settings:
    # 判断当前是否存在SCRAPY_SETTINGS_MODULE环境变量，如果不存在，则寻找scrapy.cfg配置文件的路径
    # 找到cfg文件中指定的settings配置项，将其写入SCRAPY_SETTINGS_MODULE环境变量内
    # 同时将项目目录添加到可执行路径中，主要是由init_env方法完成(具体逻辑参见)
    if ENVVAR not in os.environ:
        project = os.environ.get("SCRAPY_PROJECT", "default")
        init_env(project)

    # 初始化Settings配置对象,同时将BaseSettings中默认的基础配置项赋值给settings对象
    settings = Settings()
    settings_module_path = os.environ.get(ENVVAR)  # 获取设置的SCRAPY_SETTINGS_MODULE环境变量(在init_env方法中将会设置)
    if settings_module_path:
        # 将settings文件中的配置项全部转成大写配置项，赋值给settings对象
        settings.setmodule(settings_module_path, priority="project")
    # 定义了一些有效的环境变量
    valid_envvars = {
        "CHECK",
        "PROJECT",
        "PYTHON_SHELL",
        "SETTINGS_MODULE",
    }
    # 从当前环境变量中寻找和scrapy相关(以SCRAPY_开头)的环境变量，并且要求在_valid_envvars字典中
    scrapy_envvars = {
        k[7:]: v
        for k, v in os.environ.items()
        if k.startswith("SCRAPY_") and k.replace("SCRAPY_", "") in valid_envvars
    }

    settings.setdict(scrapy_envvars, priority="project")

    return settings
