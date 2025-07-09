import json
from tqdm import tqdm
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

load_dotenv()

class StarWarsLocalImporter:
    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI"),
            auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
        )
        
        self.data = {
            'films': self._load_and_extract('films', ['title', 'episode_id']),
            'people': self._load_and_extract('people', ['name']),
            'planets': self._load_and_extract('planets', ['name']),
            'species': self._load_and_extract('species', ['name']),
            'starships': self._load_and_extract('starships', ['name']),
            'vehicles': self._load_and_extract('vehicles', ['name'])
        }
    
    def _load_and_extract(self, entity_type: str, required_fields: list) -> List[Dict[str, Any]]:
        """Carrega os dados e extrai do objeto 'fields'"""
        file_path = f"data/raw/swapi_fixtures/{entity_type}.json"
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                return [
                    item['fields'] 
                    for item in data 
                    if isinstance(item, dict) and 
                       'fields' in item and 
                       all(field in item['fields'] for field in required_fields)
                ]
        except Exception as e:
            print(f"Erro ao carregar {entity_type}: {str(e)}")
            return []
    
    def close(self):
        self.driver.close()
    
    def import_all_data(self):
        """Orquestra a importação de todos os dados"""
        print("Iniciando importação dos dados locais...")
        
        self.import_planets()
        self.import_movies()
        self.import_species()
        self.import_starships()
        self.import_vehicles()
        self.import_characters()
        
        print("Importação concluída com sucesso!")

    def import_planets(self):
        if not self.data['planets']:
            print("Nenhum planeta encontrado para importar")
            return
            
        print(f"Importando {len(self.data['planets'])} planetas...")
        with self.driver.session() as session:
            for planet in tqdm(self.data['planets'], desc="Planetas"):
                try:
                    session.execute_write(self._create_planet, planet)
                except Exception as e:
                    print(f"Erro ao importar planeta {planet.get('name')}: {str(e)}")

    def import_movies(self):
        if not self.data['films']:
            print("Nenhum filme encontrado para importar")
            return
            
        print(f"Importando {len(self.data['films'])} filmes...")
        with self.driver.session() as session:
            for film in tqdm(self.data['films'], desc="Filmes"):
                try:
                    session.execute_write(self._create_movie, film)
                except Exception as e:
                    print(f"Erro ao importar filme {film.get('title')}: {str(e)}")

    def import_species(self):
        if not self.data['species']:
            print("Nenhuma espécie encontrada para importar")
            return
            
        print(f"Importando {len(self.data['species'])} espécies...")
        with self.driver.session() as session:
            for species in tqdm(self.data['species'], desc="Espécies"):
                try:
                    session.execute_write(self._create_species, species)
                except Exception as e:
                    print(f"Erro ao importar espécie {species.get('name')}: {str(e)}")

    def import_starships(self):
        if not self.data['starships']:
            print("Nenhuma nave estelar encontrada para importar")
            return
            
        print(f"Importando {len(self.data['starships'])} naves estelares...")
        with self.driver.session() as session:
            for starship in tqdm(self.data['starships'], desc="Naves Estelares"):
                try:
                    session.execute_write(self._create_starship, starship)
                except Exception as e:
                    print(f"Erro ao importar nave {starship.get('name')}: {str(e)}")

    def import_vehicles(self):
        if not self.data['vehicles']:
            print("Nenhum veículo encontrado para importar")
            return
            
        print(f"Importando {len(self.data['vehicles'])} veículos...")
        with self.driver.session() as session:
            for vehicle in tqdm(self.data['vehicles'], desc="Veículos"):
                try:
                    session.execute_write(self._create_vehicle, vehicle)
                except Exception as e:
                    print(f"Erro ao importar veículo {vehicle.get('name')}: {str(e)}")

    def import_characters(self):
        if not self.data['people']:
            print("Nenhum personagem encontrado para importar")
            return
            
        print(f"Importando {len(self.data['people'])} personagens...")
        with self.driver.session() as session:
            for person in tqdm(self.data['people'], desc="Personagens"):
                try:
                    session.execute_write(self._create_character, person)
                    self._link_character_relations(session, person)
                except Exception as e:
                    print(f"Erro ao importar personagem {person.get('name')}: {str(e)}")

    def _create_planet(self, tx, planet: Dict[str, Any]):
        query = """
        MERGE (p:Planet {name: $name})
        SET p.climate = $climate,
            p.terrain = $terrain,
            p.population = $population,
            p.diameter = $diameter,
            p.gravity = $gravity,
            p.rotation_period = $rotation_period,
            p.orbital_period = $orbital_period,
            p.surface_water = $surface_water
        """
        tx.run(query,
               name=planet.get('name'),
               climate=planet.get('climate', 'unknown'),
               terrain=planet.get('terrain', 'unknown'),
               population=self._parse_number(planet.get('population')),
               diameter=self._parse_number(planet.get('diameter')),
               gravity=planet.get('gravity', 'unknown'),
               rotation_period=self._parse_number(planet.get('rotation_period')),
               orbital_period=self._parse_number(planet.get('orbital_period')),
               surface_water=self._parse_number(planet.get('surface_water')))

    def _create_movie(self, tx, film: Dict[str, Any]):
        query = """
        MERGE (m:Movie {title: $title})
        SET m.episode_id = $episode_id,
            m.release_date = $release_date,
            m.director = $director,
            m.producer = $producer,
            m.opening_crawl = $opening_crawl
        """
        tx.run(query,
               title=film.get('title'),
               episode_id=film.get('episode_id'),
               release_date=film.get('release_date'),
               director=film.get('director', 'unknown'),
               producer=film.get('producer', 'unknown'),
               opening_crawl=film.get('opening_crawl', ''))

    def _create_species(self, tx, species: Dict[str, Any]):
        query = """
        MERGE (s:Species {name: $name})
        SET s.classification = $classification,
            s.designation = $designation,
            s.average_height = $average_height,
            s.average_lifespan = $average_lifespan,
            s.language = $language
        """
        tx.run(query,
               name=species.get('name'),
               classification=species.get('classification', 'unknown'),
               designation=species.get('designation', 'unknown'),
               average_height=self._parse_number(species.get('average_height')),
               average_lifespan=self._parse_number(species.get('average_lifespan')),
               language=species.get('language', 'unknown'))

    def _create_starship(self, tx, starship: Dict[str, Any]):
        query = """
        MERGE (s:Starship {name: $name})
        SET s.model = $model,
            s.manufacturer = $manufacturer,
            s.cost_in_credits = $cost_in_credits,
            s.length = $length,
            s.max_atmosphering_speed = $max_atmosphering_speed,
            s.crew = $crew,
            s.passengers = $passengers,
            s.cargo_capacity = $cargo_capacity,
            s.hyperdrive_rating = $hyperdrive_rating
        """
        tx.run(query,
               name=starship.get('name'),
               model=starship.get('model', 'unknown'),
               manufacturer=starship.get('manufacturer', 'unknown'),
               cost_in_credits=self._parse_number(starship.get('cost_in_credits')),
               length=self._parse_number(starship.get('length')),
               max_atmosphering_speed=self._parse_number(starship.get('max_atmosphering_speed')),
               crew=starship.get('crew', 'unknown'),
               passengers=starship.get('passengers', 'unknown'),
               cargo_capacity=self._parse_number(starship.get('cargo_capacity')),
               hyperdrive_rating=self._parse_number(starship.get('hyperdrive_rating')))

    def _create_vehicle(self, tx, vehicle: Dict[str, Any]):
        query = """
        MERGE (v:Vehicle {name: $name})
        SET v.model = $model,
            v.manufacturer = $manufacturer,
            v.cost_in_credits = $cost_in_credits,
            v.length = $length,
            v.max_atmosphering_speed = $max_atmosphering_speed,
            v.crew = $crew,
            v.passengers = $passengers,
            v.cargo_capacity = $cargo_capacity
        """
        tx.run(query,
               name=vehicle.get('name'),
               model=vehicle.get('model', 'unknown'),
               manufacturer=vehicle.get('manufacturer', 'unknown'),
               cost_in_credits=self._parse_number(vehicle.get('cost_in_credits')),
               length=self._parse_number(vehicle.get('length')),
               max_atmosphering_speed=self._parse_number(vehicle.get('max_atmosphering_speed')),
               crew=vehicle.get('crew', 'unknown'),
               passengers=vehicle.get('passengers', 'unknown'),
               cargo_capacity=self._parse_number(vehicle.get('cargo_capacity')))

    def _create_character(self, tx, person: Dict[str, Any]):
        query = """
        MERGE (c:Character {name: $name})
        SET c.birth_year = $birth_year,
            c.gender = $gender,
            c.height = $height,
            c.mass = $mass,
            c.eye_color = $eye_color,
            c.hair_color = $hair_color,
            c.skin_color = $skin_color
        """
        tx.run(query,
               name=person.get('name'),
               birth_year=person.get('birth_year', 'unknown'),
               gender=person.get('gender', 'unknown'),
               height=self._parse_number(person.get('height')),
               mass=self._parse_number(person.get('mass')),
               eye_color=person.get('eye_color', 'unknown'),
               hair_color=person.get('hair_color', 'unknown'),
               skin_color=person.get('skin_color', 'unknown'))

    def _link_character_relations(self, session, person: Dict[str, Any]):
        """Cria todos os relacionamentos para um personagem"""
        name = person.get('name')
        if not name:
            return

        if person.get('homeworld'):
            planet = self._find_entity_by_url('planets', person['homeworld'])
            if planet:
                session.execute_write(self._link_character_to_planet, name, planet.get('name'))

        if person.get('films'):
            for film_url in person['films']:
                film = self._find_entity_by_url('films', film_url)
                if film:
                    session.execute_write(self._link_character_to_movie, name, film.get('title'))

        if person.get('species') and person['species']:  # Verifica se não é lista vazia
            species_url = person['species'][0]  # Assume que um personagem tem apenas uma espécie principal
            species = self._find_entity_by_url('species', species_url)
            if species:
                session.execute_write(self._link_character_to_species, name, species.get('name'))

    def _link_character_to_planet(self, tx, char_name: str, planet_name: str):
        query = """
        MATCH (c:Character {name: $char_name})
        MATCH (p:Planet {name: $planet_name})
        MERGE (c)-[:FROM_PLANET]->(p)
        """
        tx.run(query, char_name=char_name, planet_name=planet_name)

    def _link_character_to_movie(self, tx, char_name: str, movie_title: str):
        query = """
        MATCH (c:Character {name: $char_name})
        MATCH (m:Movie {title: $movie_title})
        MERGE (c)-[:APPEARS_IN]->(m)
        """
        tx.run(query, char_name=char_name, movie_title=movie_title)

    def _link_character_to_species(self, tx, char_name: str, species_name: str):
        query = """
        MATCH (c:Character {name: $char_name})
        MATCH (s:Species {name: $species_name})
        MERGE (c)-[:BELONGS_TO]->(s)
        """
        tx.run(query, char_name=char_name, species_name=species_name)

    def _find_entity_by_url(self, entity_type: str, url: str) -> Optional[Dict[str, Any]]:
        """Encontra uma entidade pela URL"""
        for entity in self.data.get(entity_type, []):
            if entity.get('url') == url:
                return entity
        return None

    def _parse_number(self, value: Any) -> Optional[float]:
        """Converte valores para numérico com tratamento robusto"""
        if value is None:
            return None
        try:
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str) and value.isdigit():
                return float(value)
            if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                return float(value)
            if value == 'unknown':
                return None
        except (ValueError, TypeError):
            pass
        return None

if __name__ == "__main__":
    importer = StarWarsLocalImporter()
    try:
        importer.import_all_data()
    except Exception as e:
        print(f"Erro durante a importação: {str(e)}")
    finally:
        importer.close()