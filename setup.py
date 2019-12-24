from setuptools import setup, find_packages
import versioneer

requirements = [
    'cartopy',
    'xarray',
    'seaborn',
    'shapely',
    'dask',
    'sentinelsat',
    'toolz'
]

long_description = """
A Python package to download, preprocess and plot data from [Copernicus Open Access Hub](https://scihub.copernicus.eu). This implementation is based on `sentinelsat` [package](https://github.com/sentinelsat/sentinelsat) and the [API Hub Access](https://scihub.copernicus.eu/twiki/do/view/SciHubWebPortal/APIHubDescription) to query the database. The preprocess is made with [HARP tools](https://cdn.rawgit.com/stcorp/harp/master/doc/html/harpconvert.html).
"""

setup(
    name='s5p_tools',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="A Python package and set of scripts to download, preprocess and plot data from Sentinel-5P",
    author="Bilel Omrani",
    author_email='bilelomrani1@gmail.com',
    url='https://github.com/bilelomrani1/s5p-tools',
    packages=find_packages(),
    install_requires=requirements,
    keywords='s5p-tools',
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
    scripts=['s5p_tools/bin/s5p-request'],
    license="MIT"
)
