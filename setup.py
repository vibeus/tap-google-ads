#!/usr/bin/env python
from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="tap-google-ads",
    version="0.1.0",
    description="Singer.io tap for extracting data from Google Ads API.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Vibe Inc",
    url="https://github.com/vibeus/tap-google-ads",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_ads"],
    install_requires=[
        "google-ads",
        "requests",
        "singer-python",
    ],
    entry_points="""
    [console_scripts]
    tap-google-ads=tap_google_ads:main
    """,
    packages=["tap_google_ads", "tap_google_ads.streams"],
    package_data={"schemas": ["tap_google_ads/schemas/*.json"]},
    include_package_data=True,
)
