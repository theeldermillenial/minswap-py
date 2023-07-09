# minswap-py (v0.2.1)
<p align="center">
    <img src="https://img.shields.io/pypi/status/minswap-py?style=flat-square" />
    <img src="https://img.shields.io/pypi/dm/minswap-py?style=flat-square" />
    <img src="https://img.shields.io/pypi/l/minswap-py?style=flat-square"/>
    <img src="https://img.shields.io/pypi/v/minswap-py?style=flat-square"/>
    <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

`minswap-py` is a tool to interact with [Minswap](https://minswap.org/).  The current version has feature parity with the minswap [blockfrost-adapter](https://github.com/minswap/blockfrost-adapter).

Documentation and additional features coming soon.

This tool was recently used to help generate data for the Minswap DAO Emissions and
Treasury report. You can read the report here:

https://minswap.org/storage/2023/06/31-3-2023_Emissions_and_Treasury_Report.pdf

## Have a question?

Reach out to me on the Minswap discord. You can usually find me on`#technical`, and I'm happy to respond to questions there.

https://discord.com/channels/829060079248343090/845208119729979402

## Support

If you find this project useful, please consider supporting the project by buying me a
beer in the form of ADA or MIN:

```bash
addr1q9hw8fuex09vr3rqwtn4fzh9qxjlzjzh8aww684ln0rv0cfu3f0de6qkmh7c7yysfz808978wwe6ll30wu8l3cgvgdjqa7egnl
```

## Changelog

Added CHANGELOG.md :)

Improvements:
1. Added a blockfrost rate limiter. Every call to blockfrost adds to a time delayed counter, and prevents running into rate limits. This helps to prevent abuse of the blockfrost API.
2. Added significant transaction and asset functionality. It is now possible to pull in asset historys.

Changes:
1. There was an inconsistency in how time values were being cached. See the `examples/rename_time.py` for a way to translate previously cached data to the new standard.

## Quickstart

In order to use this package:
1. Install with `pip install minswap-py`
2. Sign up for blockfrost and get an API key.
3. In your working directory, save a `.env` file. In this file, save your blockfrost API key as follows:
```bash
PROJECT_ID=YOUR_BLOCKFROST_ID
```

Once you do this, you can try out the code in the `examples` folder on the Github repository.
