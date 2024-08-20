
## Setup

```sh
docker build -t postgresdb .
```

```sh
docker run --name postgresdb -v $(pwd)/data:/data -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres 
```