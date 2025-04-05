from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='myFirstProject-package',
    version='1.0.0',
    packages=find_packages(),
    description='Descricao da sua lib myFirstProject',
    author='Iury R. Miguel',
    author_email='iurymig.sht@gmail.com',
    url='https://github.com/tadrianonet/myFirstProject',  
    license='MIT',  
    long_description=long_description,
    long_description_content_type='text/markdown'
)
