#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PROYECTO: Gestión de Nuevos Hits Musicales en una Discográfica
MÓDULO: Generador de Datos de Prueba (Seed Data)
DESCRIPCIÓN: Script que genera datos de alto volumen para poblar la base de datos
REQUISITOS: pip install faker psycopg2-binary
"""

import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import logging

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de conexión a PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'music_label_db',
    'user': 'music_user',
    'password': 'password123',
    'port': 5432
}

# Inicializar Faker
fake = Faker('es_ES')
Faker.seed(42)
random.seed(42)

class DataGenerator:
    def __init__(self):
        self.conn = None
        self.cur = None
        self.artist_ids = []
        self.producer_ids = []
        self.studio_ids = []
        self.album_ids = []
        self.distribution_ids = []
        self.staff_ids = []
        self.engineer_ids = []
        self.manager_ids = []

    def connect(self):
        """Conectar a la base de datos PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.cur = self.conn.cursor()
            logger.info("✓ Conexión a PostgreSQL establecida")
        except Exception as e:
            logger.error(f"✗ Error al conectar: {e}")
            raise

    def disconnect(self):
        """Desconectar de la base de datos"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("✓ Conexión cerrada")

    def execute_query(self, query, params=None):
        """Ejecutar una consulta y retornar el resultado solo si existen datos"""
        try:
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            self.conn.commit()

            # SOLUCIÓN: Solo hacemos fetch si la base de datos dice que hay filas para leer
            if self.cur.description:
                return self.cur.fetchall()
            return None

        except Exception as e:
            self.conn.rollback()
            # logger.error(f"✗ Error en consulta: {e}") # Puedes descomentar esto si quieres ver el error en detalle
            raise e

    def clean_database(self):
        """Limpiar datos existentes antes de poblar"""
        logger.info("Limpiando base de datos...")
        tables = [
            'song_distribution', 'artist_album', 'contract', 'recording_session',
            'song', 'album', 'engineer', 'manager', 'staff',
            'distribution', 'studio', 'producer', 'artist'
        ]

        try:
            for table in tables:
                # CASCADE permite borrar aunque tengan relaciones
                self.execute_query(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            logger.info("✓ Base de datos limpiada correctamente")
        except Exception as e:
            logger.error(f"✗ Error limpiando base de datos: {e}")

    def generate_artists(self, count=60):
        """Generar artistas"""
        logger.info(f"Generando {count} artistas...")
        genres = ['pop', 'rock', 'hip-hop', 'jazz', 'classical', 'electronic', 'reggae', 'country', 'folk', 'other']
        nationalities = ['España', 'Estados Unidos', 'Reino Unido', 'Francia', 'Alemania', 'Italia', 'Japón', 'Brasil', 'Canadá', 'Australia']

        for i in range(count):
            name = fake.name()
            genre = random.choice(genres)
            contract_date = fake.date_between(start_date='-5y')
            contract_status = random.choice(['active', 'inactive', 'suspended'])
            biography = fake.paragraph(nb_sentences=3)
            nationality = random.choice(nationalities)

            query = """
                INSERT INTO artist (name, genre, contract_date, contract_status, biography, nationality)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING artist_id
            """
            result = self.execute_query(query, (name, genre, contract_date, contract_status, biography, nationality))
            self.artist_ids.append(result[0][0])

        logger.info(f"✓ {count} artistas generados")

    def generate_producers(self, count=50):
        """Generar productores"""
        logger.info(f"Generando {count} productores...")
        specialties = ['Pop', 'Rock', 'Hip-Hop', 'Jazz', 'Electronic', 'Classical', 'Reggae', 'Country']

        for i in range(count):
            name = fake.name()
            specialty = random.choice(specialties)
            years_experience = random.randint(1, 40)
            email = fake.email()
            phone = fake.phone_number()

            query = """
                INSERT INTO producer (name, specialty, years_experience, email, phone)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING producer_id
            """
            result = self.execute_query(query, (name, specialty, years_experience, email, phone))
            self.producer_ids.append(result[0][0])

        logger.info(f"✓ {count} productores generados")

    def generate_studios(self, count=20):
        """Generar estudios de grabación"""
        logger.info(f"Generando {count} estudios...")
        locations = ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Bilbao', 'Málaga', 'Murcia', 'Palma', 'Las Palmas', 'Alicante']
        equipment_list = [
            'SSL 4000E, Neve 1073, Telefunken U47',
            'Neve 8088, API 2500, Manley ELOP',
            'Studer A800, Ampex ATR102, Otari MX80',
            'ProTools HDX, Avid S6, Euphonix MC Mix',
            'Yamaha CL5, Allen & Heath dLive, DiGiCo SD7'
        ]

        for i in range(count):
            name = f"Studio {fake.word().capitalize()} {i+1}"
            location = random.choice(locations)
            capacity = random.randint(4, 30)
            equipment = random.choice(equipment_list)
            hourly_rate = round(random.uniform(50, 300), 2)

            query = """
                INSERT INTO studio (name, location, capacity, equipment, hourly_rate)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING studio_id
            """
            result = self.execute_query(query, (name, location, capacity, equipment, hourly_rate))
            self.studio_ids.append(result[0][0])

        logger.info(f"✓ {count} estudios generados")

    def generate_albums(self, count=100):
        """Generar álbumes"""
        logger.info(f"Generando {count} álbumes...")
        genres = ['pop', 'rock', 'hip-hop', 'jazz', 'classical', 'electronic', 'reggae', 'country', 'folk', 'other']
        statuses = ['draft', 'released', 'archived']

        for i in range(count):
            title = f"{fake.word().capitalize()} {fake.word().capitalize()}"
            release_date = fake.date_between(start_date='-3y')
            producer_id = random.choice(self.producer_ids)
            status = random.choice(statuses)
            genre = random.choice(genres)
            label_name = fake.company()

            query = """
                INSERT INTO album (title, release_date, producer_id, status, genre, label_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING album_id
            """
            result = self.execute_query(query, (title, release_date, producer_id, status, genre, label_name))
            self.album_ids.append(result[0][0])

        logger.info(f"✓ {count} álbumes generados")

    def generate_songs(self, count=500):
        """Generar canciones (entidad débil)"""
        logger.info(f"Generando {count} canciones...")

        # 1. Desactivar validación de Hits
        try:
            self.execute_query("ALTER TABLE song DISABLE TRIGGER trg_check_hit_distributions")
        except Exception as e:
            logger.warning(f"No se pudo desactivar trigger (posiblemente no exista): {e}")

        inserted_count = 0
        for i in range(count):
            album_id = random.choice(self.album_ids)
            song_id = random.randint(1, 20)
            title = f"{fake.word().capitalize()} {fake.word().capitalize()}"
            duration = random.randint(120, 420)
            composer = fake.name()
            streams_count = random.randint(0, 10000000)
            is_hit = streams_count >= 1000000

            # Nota: Este INSERT no tiene 'RETURNING', ahora funcionará bien gracias al cambio en execute_query
            query = """
                INSERT INTO song (album_id, song_id, title, duration, composer, streams_count, is_hit)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            try:
                self.execute_query(query, (album_id, song_id, title, duration, composer, streams_count, is_hit))
                inserted_count += 1
            except Exception:
                pass # Ignorar duplicados o errores puntuales

        # 2. Reactivar validación
        try:
            self.execute_query("ALTER TABLE song ENABLE TRIGGER trg_check_hit_distributions")
        except:
            pass

        logger.info(f"✓ {inserted_count} canciones generadas (intentadas: {count})")

    def generate_recording_sessions(self, count=150):
        """Generar sesiones de grabación (relación ternaria)"""
        logger.info(f"Generando {count} sesiones de grabación...")

        for i in range(count):
            artist_id = random.choice(self.artist_ids)
            producer_id = random.choice(self.producer_ids)
            studio_id = random.choice(self.studio_ids)
            session_date = fake.date_between(start_date='-2y')
            start_time = f"{random.randint(9, 18):02d}:{random.randint(0, 59):02d}:00"
            duration_hours = round(random.uniform(1, 8), 2)
            # cost será calculado automáticamente por el trigger

            query = """
                INSERT INTO recording_session (artist_id, producer_id, studio_id, session_date, start_time, duration_hours, cost)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            try:
                self.execute_query(query, (artist_id, producer_id, studio_id, session_date, start_time, duration_hours, 0))
            except:
                pass

        logger.info(f"✓ {count} sesiones de grabación generadas")

    def generate_staff(self, count=40):
        """Generar personal (staff)"""
        logger.info(f"Generando {count} empleados...")
        departments = ['Ingeniería', 'Producción', 'Administración', 'Marketing', 'Distribución', 'Legal']

        for i in range(count):
            name = fake.name()
            salary = round(random.uniform(20000, 80000), 2)
            department = random.choice(departments)
            hire_date = fake.date_between(start_date='-10y')

            query = """
                INSERT INTO staff (name, salary, department, hire_date)
                VALUES (%s, %s, %s, %s)
                RETURNING staff_id
            """
            result = self.execute_query(query, (name, salary, department, hire_date))
            self.staff_ids.append(result[0][0])

        logger.info(f"✓ {count} empleados generados")

    def generate_engineers(self, count=15):
        """Generar ingenieros (especialización IS_A)"""
        logger.info(f"Generando {count} ingenieros...")
        certifications = ['Certified Audio Engineer', 'Pro Tools Certified', 'Neve Certified', 'SSL Certified', 'Dolby Certified']

        for i in range(min(count, len(self.staff_ids))):
            engineer_id = self.staff_ids[i]
            certification = random.choice(certifications)
            technical_skills = fake.paragraph(nb_sentences=2)
            studio_id = random.choice(self.studio_ids)

            query = """
                INSERT INTO engineer (engineer_id, certification, technical_skills, studio_id)
                VALUES (%s, %s, %s, %s)
            """
            try:
                self.execute_query(query, (engineer_id, certification, technical_skills, studio_id))
                self.engineer_ids.append(engineer_id)
            except:
                pass

        logger.info(f"✓ {count} ingenieros generados")

    def generate_managers(self, count=10):
        """Generar gerentes (especialización IS_A)"""
        logger.info(f"Generando {count} gerentes...")

        for i in range(count, min(count + 10, len(self.staff_ids))):
            manager_id = self.staff_ids[i]
            commission_percentage = round(random.uniform(5, 25), 2)
            artists_managed = random.randint(1, 10)

            query = """
                INSERT INTO manager (manager_id, commission_percentage, artists_managed)
                VALUES (%s, %s, %s)
            """
            try:
                self.execute_query(query, (manager_id, commission_percentage, artists_managed))
                self.manager_ids.append(manager_id)
            except:
                pass

        logger.info(f"✓ {count} gerentes generados")

    def generate_distributions(self, count=10):
        """Generar plataformas de distribución"""
        logger.info(f"Generando {count} plataformas de distribución...")
        platforms = [
            'Spotify', 'Apple Music', 'YouTube Music', 'Amazon Music', 'Tidal',
            'Deezer', 'SoundCloud', 'Bandcamp', 'Beatport', 'iTunes'
        ]

        for platform in platforms[:count]:
            commission_percentage = round(random.uniform(15, 50), 2)
            agreement_status = random.choice(['active', 'inactive', 'pending'])

            query = """
                INSERT INTO distribution (platform_name, commission_percentage, agreement_status)
                VALUES (%s, %s, %s)
                RETURNING distribution_id
            """
            try:
                result = self.execute_query(query, (platform, commission_percentage, agreement_status))
                self.distribution_ids.append(result[0][0])
            except:
                pass

        logger.info(f"✓ {count} plataformas de distribución generadas")

    def generate_song_distributions(self, count=300):
        """Generar relaciones M:M entre canciones y plataformas"""
        logger.info(f"Generando {count} distribuciones de canciones...")

        for i in range(count):
            try:
                # Obtener una canción aleatoria
                query = "SELECT album_id, song_id FROM song ORDER BY RANDOM() LIMIT 1"
                result = self.execute_query(query)
                if result:
                    album_id, song_id = result[0]
                    distribution_id = random.choice(self.distribution_ids)
                    publish_date = fake.date_between(start_date='-2y')
                    status = random.choice(['active', 'inactive', 'pending'])

                    insert_query = """
                        INSERT INTO song_distribution (song_id, album_id, distribution_id, publish_date, status)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """
                    self.execute_query(insert_query, (song_id, album_id, distribution_id, publish_date, status))
            except:
                pass

        logger.info(f"✓ {count} distribuciones de canciones generadas")

    def generate_contracts(self, count=50):
        """Generar contratos"""
        logger.info(f"Generando {count} contratos...")

        for i in range(min(count, len(self.artist_ids))):
            artist_id = self.artist_ids[i]
            start_date = fake.date_between(start_date='-5y')
            end_date = start_date + timedelta(days=random.randint(365, 1825))
            royalty_percentage = round(random.uniform(5, 25), 2)
            contract_terms = fake.paragraph(nb_sentences=3)
            status = 'active' if end_date >= datetime.now().date() else 'expired'

            query = """
                INSERT INTO contract (artist_id, start_date, end_date, royalty_percentage, contract_terms, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """
            try:
                self.execute_query(query, (artist_id, start_date, end_date, royalty_percentage, contract_terms, status))
            except:
                pass

        logger.info(f"✓ {count} contratos generados")

    def generate_artist_albums(self, count=150):
        """Generar relaciones M:M entre artistas y álbumes"""
        logger.info(f"Generando {count} relaciones artista-álbum...")

        for i in range(count):
            try:
                artist_id = random.choice(self.artist_ids)
                album_id = random.choice(self.album_ids)
                role = random.choice(['featured', 'collaborator', 'featured artist', 'guest'])

                query = """
                    INSERT INTO artist_album (artist_id, album_id, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                self.execute_query(query, (artist_id, album_id, role))
            except:
                pass

        logger.info(f"✓ {count} relaciones artista-álbum generadas")

    def generate_all(self):
        """Ejecutar generación completa de datos"""
        try:
            self.connect()
            logger.info("=" * 60)
            logger.info("INICIANDO GENERACIÓN DE DATOS DE PRUEBA")
            logger.info("=" * 60)

            self.clean_database()
            self.generate_artists(60)
            self.generate_producers(50)
            self.generate_studios(20)
            self.generate_albums(100)
            self.generate_songs(500)
            self.generate_recording_sessions(150)
            self.generate_staff(40)
            self.generate_engineers(15)
            self.generate_managers(10)
            self.generate_distributions(10)
            self.generate_song_distributions(300)
            self.generate_contracts(50)
            self.generate_artist_albums(150)

            logger.info("=" * 60)
            logger.info("✓ GENERACIÓN DE DATOS COMPLETADA EXITOSAMENTE")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"✗ Error durante la generación: {e}")
        finally:
            self.disconnect()

if __name__ == '__main__':
    generator = DataGenerator()
    generator.generate_all()
