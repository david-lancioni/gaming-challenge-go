@cls
@echo off
rem ==========================================================================
rem  startup.bat - sobe o projeto no Windows (cmd.exe)
rem
rem    startup.bat          tudo no Docker (Postgres, LocalStack, Keycloak, 3 APIs)
rem    startup.bat local    dependencias no Docker, servico Go rodando no host
rem    startup.bat stop     para tudo e apaga os volumes
rem    startup.bat logs     acompanha os logs dos containers
rem ==========================================================================
setlocal
cd /d "%~dp0"

set MODE=%~1
if "%MODE%"=="" set MODE=docker

if /i "%MODE%"=="stop" goto skip_warning
if /i "%MODE%"=="logs" goto skip_warning
echo ==========================================================================
echo  ATENCAO: antes de continuar, pare/remova qualquer container antigo deste
echo  projeto (ou de outros) que use as portas 5432, 4566, 8080, 8081-8083 ou
echo  9101-9103. Caso contrario a subida falha com "port is already allocated".
echo.
echo  Para listar:   docker ps -a
echo  Para remover:  docker compose -p NOME_DO_PROJETO down
echo                 (ex.: docker compose -p backend-challenge-go down)
echo ==========================================================================
echo.
set /p "_=Pressione ENTER para continuar (ou Ctrl+C para cancelar)..."
echo.
:skip_warning

where docker >nul 2>&1 || (
  echo [ERRO] Docker nao encontrado. Instale o Docker Desktop e tente de novo.
  exit /b 1
)
docker info >nul 2>&1 || (
  echo [ERRO] O Docker nao esta rodando. Abra o Docker Desktop e tente de novo.
  exit /b 1
)

if /i "%MODE%"=="docker" goto docker
if /i "%MODE%"=="local"  goto local
if /i "%MODE%"=="stop"   goto stop
if /i "%MODE%"=="logs"   goto logs
echo Uso: %~nx0 [docker^|local^|stop^|logs]
exit /b 1

rem --------------------------------------------------------------------------
:docker
echo === Subindo todos os servicos no Docker...
docker compose up --build -d || exit /b 1

echo === Aguardando a API ficar pronta (http://localhost:8081/health/ready)...
set /a TRIES=0
:wait_api
curl -fs http://localhost:8081/health/ready >nul 2>&1 && goto api_ready
set /a TRIES+=1
if %TRIES% geq 60 (
  echo [ERRO] A API nao respondeu em 5 minutos. Veja os logs: %~nx0 logs
  exit /b 1
)
timeout /t 5 /nobreak >nul
goto wait_api

:api_ready
echo.
echo === Pronto!
echo   API 1      http://localhost:8081   metricas http://localhost:9101/metrics
echo   API 2      http://localhost:8082   metricas http://localhost:9102/metrics
echo   API 3      http://localhost:8083   metricas http://localhost:9103/metrics
echo   Keycloak   http://localhost:8080   (admin/admin)
echo.
echo   Logs:  %~nx0 logs      Parar:  %~nx0 stop
start "" http://localhost:8081/health/ready
exit /b 0

rem --------------------------------------------------------------------------
:local
where go >nul 2>&1 || (
  echo [ERRO] Go nao encontrado. Instale o Go 1.26 ou use: %~nx0 docker
  exit /b 1
)

echo === Subindo dependencias (Postgres, LocalStack, Keycloak) e aguardando healthy...
docker compose up -d --wait postgres localstack keycloak || exit /b 1

set DATABASE_URL=postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable
set AWS_ENDPOINT_URL=http://localhost:4566
set AWS_ACCESS_KEY_ID=test
set AWS_SECRET_ACCESS_KEY=test
set OIDC_ISSUER=http://localhost:8080/realms/wagering
set OIDC_JWKS_URL=http://localhost:8080/realms/wagering/protocol/openid-connect/certs
set HTTP_ADDR=:8081
set METRICS_ADDR=:9101

echo === Aplicando migrations...
go run ./cmd/wagering migrate up || exit /b 1

echo === Criando filas SQS...
go run ./cmd/wagering queues init || exit /b 1

echo.
echo === Iniciando o servico em http://localhost:8081 (Ctrl+C para parar)
go run ./cmd/wagering serve
exit /b %ERRORLEVEL%

rem --------------------------------------------------------------------------
:stop
echo === Parando e removendo containers e volumes...
docker compose down -v
exit /b %ERRORLEVEL%

rem --------------------------------------------------------------------------
:logs
docker compose logs -f
exit /b %ERRORLEVEL%
