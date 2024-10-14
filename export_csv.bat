@echo off
REM Prompt the user for the three variables

set "default_filepath=%cd%"
set /p username=Digite seu usuario:
set /p password=Digite sua senha: 
set /p query=Cole a query MINIFICADA: 
set /p filename=Digite o nome do arquivo: 
set /p filepath=Digite o caminho (Default: %default_filepath%):
if "%filepath%"=="" set "filepath=%default_filepath%"

REM Display the values for verification
echo.
echo Username: %username%
echo Password: %password%
echo Query: %query%
echo Filename: %filename%
echo Filepath: %filepath%

REM Run the Python script with the collected values
python redshift_file_generator_by_chunks.py --username "%username%" --password "%password%" --query "%query%" --filename "%filename%" --filepath "%filepath%"

REM Wait for user input before closing (optional)
pause