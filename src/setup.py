import setuptools

setuptools.setup(
    name='canvas',
    version='1.0',
    author='Marcos Alcozer',
    author_email='marcos@alcozer.dev',
    url='https://www.alcozer.dev',
    install_requires=[
        'tenacity==7.0.0'
    ],
    packages=setuptools.find_packages()
)
