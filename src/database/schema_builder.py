import json
from neo4j import GraphDatabase
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

load_dotenv()

class StarWarsGraphBuilder:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        )

    def close(self):
        self.driver.close()

    def clear_database(self):
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

    def create_constraints_and_indexes(self):
        """Cria constraints de unicidade e índices para melhorar performance"""
        constraints = [
            "CREATE CONSTRAINT character_name IF NOT EXISTS FOR (c:Character) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT planet_name IF NOT EXISTS FOR (p:Planet) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT movie_title IF NOT EXISTS FOR (m:Movie) REQUIRE m.title IS UNIQUE",
            "CREATE CONSTRAINT species_name IF NOT EXISTS FOR (s:Species) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT starship_name IF NOT EXISTS FOR (ss:Starship) REQUIRE ss.name IS UNIQUE",
            "CREATE CONSTRAINT vehicle_name IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.name IS UNIQUE",
            "CREATE CONSTRAINT weapon_name IF NOT EXISTS FOR (w:Weapon) REQUIRE w.name IS UNIQUE",
            "CREATE CONSTRAINT organization_name IF NOT EXISTS FOR (o:Organization) REQUIRE o.name IS UNIQUE",
            
            "CREATE INDEX character_birth_year IF NOT EXISTS FOR (c:Character) ON (c.birth_year)",
            "CREATE INDEX movie_release_year IF NOT EXISTS FOR (m:Movie) ON (m.release_year)",
            "CREATE INDEX planet_climate IF NOT EXISTS FOR (p:Planet) ON (p.climate)",
            "CREATE INDEX species_classification IF NOT EXISTS FOR (s:Species) ON (s.classification)"
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                session.run(constraint)

    def create_schema(self):
        """Define o esquema completo do grafo Star Wars"""
        schema_queries = [
            """
            CREATE (:Character {
                name: 'Exemplo Personagem',
                birth_year: '19BBY',
                gender: 'male',
                height: 172,
                mass: 77,
                eye_color: 'blue',
                hair_color: 'blond',
                skin_color: 'fair'
            })
            """,
            
            """
            CREATE (:Planet {
                name: 'Exemplo Planeta',
                climate: 'temperate',
                terrain: 'grasslands, mountains',
                population: 200000,
                diameter: 12500,
                gravity: '1 standard',
                rotation_period: 24
            })
            """,
            
            """
            CREATE (:Movie {
                title: 'Exemplo Filme',
                episode_id: 4,
                release_year: 1977,
                director: 'George Lucas',
                producer: 'Gary Kurtz',
                opening_crawl: 'É um período de guerra civil...'
            })
            """,
            
            """
            CREATE (:Species {
                name: 'Exemplo Espécie',
                classification: 'mammal',
                designation: 'sentient',
                average_height: 180,
                average_lifespan: 120,
                language: 'Galactic Basic'
            })
            """,
            
            """
            CREATE (:Starship {
                name: 'Exemplo Nave',
                model: 'T-65 X-wing',
                manufacturer: 'Incom Corporation',
                cost_in_credits: 149999,
                length: 12.5,
                max_atmosphering_speed: 1050,
                crew: 1,
                passengers: 0,
                hyperdrive_rating: 1.0
            })
            """,
            
            """
            CREATE (:Vehicle {
                name: 'Exemplo Veículo',
                model: 'Digger Crawler',
                manufacturer: 'Corellia Mining Corporation',
                cost_in_credits: 75000,
                length: 36.8,
                max_atmosphering_speed: 30,
                crew: 46,
                passengers: 30
            })
            """,
            
            """
            CREATE (:Weapon {
                name: 'Exemplo Arma',
                type: 'lightsaber',
                model: 'Lightsaber',
                manufacturer: 'Jedi Order',
                cost_in_credits: 10000,
                length: 1.5,
                weight: 1.2
            })
            """,
            
            """
            CREATE (:Organization {
                name: 'Exemplo Organização',
                type: 'military',
                alignment: 'dark',
                founding_year: '100BBY',
                headquarters: 'Exemplo Planeta'
            })
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (p:Planet {name: 'Exemplo Planeta'})
            CREATE (c)-[:FROM_PLANET]->(p)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (m:Movie {title: 'Exemplo Filme'})
            CREATE (c)-[:APPEARS_IN]->(m)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (s:Species {name: 'Exemplo Espécie'})
            CREATE (c)-[:BELONGS_TO]->(s)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (ss:Starship {name: 'Exemplo Nave'})
            CREATE (c)-[:PILOTS]->(ss)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (v:Vehicle {name: 'Exemplo Veículo'})
            CREATE (c)-[:DRIVES]->(v)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (w:Weapon {name: 'Exemplo Arma'})
            CREATE (c)-[:WIELDS]->(w)
            """,
            
            """
            MATCH (c:Character {name: 'Exemplo Personagem'})
            MATCH (o:Organization {name: 'Exemplo Organização'})
            CREATE (c)-[:MEMBER_OF]->(o)
            """,
            
            """
            MATCH (p:Planet {name: 'Exemplo Planeta'})
            MATCH (m:Movie {title: 'Exemplo Filme'})
            CREATE (p)-[:APPEARS_IN]->(m)
            """,
            
            """
            MATCH (n)
            WHERE n.name STARTS WITH 'Exemplo'
            DETACH DELETE n
            """
        ]

        with self.driver.session() as session:
            for query in schema_queries:
                session.run(query)

    def build_graph(self):
        """Executa todo o processo de construção do grafo"""
        print("Limpando banco de dados existente...")
        self.clear_database()
        
        print("Criando constraints e índices...")
        self.create_constraints_and_indexes()
        
        print("Definindo esquema do grafo...")
        self.create_schema()
        
        print("Esquema Star Wars criado com sucesso!")

if __name__ == "__main__":
    builder = StarWarsGraphBuilder()
    try:
        builder.build_graph()
    finally:
        builder.close()