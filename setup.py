from setuptools import setup


setup(
    name="videosdl",
    version="1.0.0",
    py_modules=['async_all', 'asynchttpdownloader', 'asynchlsdownloader', 'videodownloader', 'asynclogger', 'common_utils', 'colargulog'],
    entry_points={"console_scripts": ["videosdl=async_all:main"]}
)
