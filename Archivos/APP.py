#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PROYECTO: Gestión de Nuevos Hits Musicales en una Discográfica
MÓDULO: API REST Flask
DESCRIPCIÓN: API REST para operaciones CRUD sobre la base de datos
REQUISITOS: pip install flask psycopg2-binary flask-cors
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar Flask
app = Flask(__name__)
CORS(app)

# Configuración de conexión a PostgreSQL
DB_CONFIG = {
    'host': 'localhost',
    'database': 'music_label_db',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def get_db_connection():
    """Obtener conexión a la base de datos"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        return None

def execute_query(query, params=None, fetch_one=False):
    """Ejecutar una consulta y retornar resultados"""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)

        if query.strip().upper().startswith('SELECT'):
            if fetch_one:
                result = cur.fetchone()
            else:
                result = cur.fetchall()
        else:
            conn.commit()
            result = cur.rowcount

        cur.close()
        return result
    except Exception as e:
        logger.error(f"Error en consulta: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

# ============================================================================
# RUTAS - ARTISTAS
# ============================================================================

@app.route('/api/artists', methods=['GET'])
def get_artists():
    """Obtener lista de artistas"""
    query = """
        SELECT artist_id, name, genre, contract_date, contract_status, biography, nationality
        FROM artist
        ORDER BY name
        LIMIT 100
    """
    result = execute_query(query)
    if result is None:
        return jsonify({'error': 'Error al obtener artistas'}), 500
    return jsonify([dict(row) for row in result])

@app.route('/api/artists/<int:artist_id>', methods=['GET'])
def get_artist(artist_id):
    """Obtener un artista específico"""
    query = """
        SELECT artist_id, name, genre, contract_date, contract_status, biography, nationality
        FROM artist
        WHERE artist_id = %s
    """
    result = execute_query(query, (artist_id,), fetch_one=True)
    if result is None:
        return jsonify({'error': 'Artista no encontrado'}), 404
    return jsonify(dict(result))

@app.route('/api/artists', methods=['POST'])
def create_artist():
    """Crear un nuevo artista"""
    data = request.get_json()
    required_fields = ['name', 'genre', 'contract_date', 'nationality']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Campos requeridos faltantes'}), 400

    query = """
        INSERT INTO artist (name, genre, contract_date, contract_status, biography, nationality)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING artist_id
    """
    params = (
        data['name'],
        data['genre'],
        data['contract_date'],
        data.get('contract_status', 'active'),
        data.get('biography', ''),
        data['nationality']
    )

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params)
        result = cur.fetchone()
        conn.commit()
        return jsonify({'artist_id': result['artist_id'], 'message': 'Artista creado exitosamente'}), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error: {e}")
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

@app.route('/api/artists/<int:artist_id>', methods=['PUT'])
def update_artist(artist_id):
    """Actualizar un artista"""
    data = request.get_json()
    allowed_fields = ['name', 'genre', 'contract_status', 'biography', 'nationality']

    updates = []
    params = []
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return jsonify({'error': 'No hay campos para actualizar'}), 400

    params.append(artist_id)
    query = f"UPDATE artist SET {', '.join(updates)} WHERE artist_id = %s"

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return jsonify({'message': 'Artista actualizado exitosamente'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

@app.route('/api/artists/<int:artist_id>', methods=['DELETE'])
def delete_artist(artist_id):
    """Eliminar un artista"""
    query = "DELETE FROM artist WHERE artist_id = %s"

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor()
        cur.execute(query, (artist_id,))
        conn.commit()
        return jsonify({'message': 'Artista eliminado exitosamente'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

# ============================================================================
# RUTAS - ÁLBUMES
# ============================================================================

@app.route('/api/albums', methods=['GET'])
def get_albums():
    """Obtener lista de álbumes"""
    query = """
        SELECT a.album_id, a.title, a.release_date, a.producer_id, p.name as producer_name,
               a.status, a.genre, a.label_name
        FROM album a
        LEFT JOIN producer p ON a.producer_id = p.producer_id
        ORDER BY a.release_date DESC
        LIMIT 100
    """
    result = execute_query(query)
    if result is None:
        return jsonify({'error': 'Error al obtener álbumes'}), 500
    return jsonify([dict(row) for row in result])

@app.route('/api/albums/<int:album_id>', methods=['GET'])
def get_album(album_id):
    """Obtener un álbum específico"""
    query = """
        SELECT a.album_id, a.title, a.release_date, a.producer_id, p.name as producer_name,
               a.status, a.genre, a.label_name
        FROM album a
        LEFT JOIN producer p ON a.producer_id = p.producer_id
        WHERE a.album_id = %s
    """
    result = execute_query(query, (album_id,), fetch_one=True)
    if result is None:
        return jsonify({'error': 'Álbum no encontrado'}), 404
    return jsonify(dict(result))

@app.route('/api/albums', methods=['POST'])
def create_album():
    """Crear un nuevo álbum"""
    data = request.get_json()
    required_fields = ['title', 'release_date', 'producer_id', 'genre']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Campos requeridos faltantes'}), 400

    query = """
        INSERT INTO album (title, release_date, producer_id, status, genre, label_name)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING album_id
    """
    params = (
        data['title'],
        data['release_date'],
        data['producer_id'],
        data.get('status', 'draft'),
        data['genre'],
        data.get('label_name', '')
    )

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params)
        result = cur.fetchone()
        conn.commit()
        return jsonify({'album_id': result['album_id'], 'message': 'Álbum creado exitosamente'}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

@app.route('/api/albums/<int:album_id>', methods=['PUT'])
def update_album(album_id):
    """Actualizar un álbum"""
    data = request.get_json()
    allowed_fields = ['title', 'status', 'genre', 'label_name']

    updates = []
    params = []
    for field in allowed_fields:
        if field in data:
            updates.append(f"{field} = %s")
            params.append(data[field])

    if not updates:
        return jsonify({'error': 'No hay campos para actualizar'}), 400

    params.append(album_id)
    query = f"UPDATE album SET {', '.join(updates)} WHERE album_id = %s"

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        return jsonify({'message': 'Álbum actualizado exitosamente'}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

# ============================================================================
# RUTAS - CANCIONES
# ============================================================================

@app.route('/api/songs', methods=['GET'])
def get_songs():
    """Obtener lista de canciones"""
    album_id = request.args.get('album_id')
    query = """
        SELECT s.song_id, s.album_id, s.title, s.duration, s.composer,
               s.streams_count, s.is_hit, a.title as album_title
        FROM song s
        JOIN album a ON s.album_id = a.album_id
    """
    params = []

    if album_id:
        query += " WHERE s.album_id = %s"
        params.append(album_id)

    query += " ORDER BY s.album_id, s.song_id LIMIT 200"
    result = execute_query(query, params if params else None)
    if result is None:
        return jsonify({'error': 'Error al obtener canciones'}), 500
    return jsonify([dict(row) for row in result])

@app.route('/api/songs/hits', methods=['GET'])
def get_hit_songs():
    """Obtener solo canciones exitosas (hits)"""
    query = """
        SELECT s.song_id, s.album_id, s.title, s.streams_count, a.title as album_title,
               COUNT(DISTINCT sd.distribution_id) as num_platforms
        FROM song s
        JOIN album a ON s.album_id = a.album_id
        LEFT JOIN song_distribution sd ON s.album_id = sd.album_id AND s.song_id = sd.song_id
        WHERE s.is_hit = TRUE
        GROUP BY s.song_id, s.album_id, s.title, s.streams_count, a.title
        ORDER BY s.streams_count DESC
        LIMIT 50
    """
    result = execute_query(query)
    if result is None:
        return jsonify({'error': 'Error al obtener canciones hit'}), 500
    return jsonify([dict(row) for row in result])

@app.route('/api/songs', methods=['POST'])
def create_song():
    """Crear una nueva canción"""
    data = request.get_json()
    required_fields = ['album_id', 'song_id', 'title', 'duration', 'composer']

    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Campos requeridos faltantes'}), 400

    query = """
        INSERT INTO song (album_id, song_id, title, duration, composer, streams_count, is_hit)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING song_id, album_id
    """
    params = (
        data['album_id'],
        data['song_id'],
        data['title'],
        data['duration'],
        data['composer'],
        data.get('streams_count', 0),
        data.get('is_hit', False)
    )

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Error de conexión'}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(query, params)
        result = cur.fetchone()
        conn.commit()
        return jsonify({'song_id': result['song_id'], 'album_id': result['album_id'], 'message': 'Canción creada exitosamente'}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 400
    finally:
        cur.close()
        conn.close()

# ============================================================================
# RUTAS - PRODUCTORES
# ============================================================================

@app.route('/api/producers', methods=['GET'])
def get_producers():
    """Obtener lista de productores"""
    query = """
        SELECT p.producer_id, p.name, p.specialty, p.years_experience, p.email, p.phone,
               COUNT(DISTINCT a.album_id) as num_albums
        FROM producer p
        LEFT JOIN album a ON p.producer_id = a.producer_id
        GROUP BY p.producer_id, p.name, p.specialty, p.years_experience, p.email, p.phone
        ORDER BY num_albums DESC
        LIMIT 100
    """
    result = execute_query(query)
    if result is None:
        return jsonify({'error': 'Error al obtener productores'}), 500
    return jsonify([dict(row) for row in result])

@app.route('/api/producers/<int:producer_id>', methods=['GET'])
def get_producer(producer_id):
    """Obtener un productor específico"""
    query = """
        SELECT p.producer_id, p.name, p.specialty, p.years_experience, p.email, p.phone
        FROM producer p
        WHERE p.producer_id = %s
    """
    result = execute_query(query, (producer_id,), fetch_one=True)
    if result is None:
        return jsonify({'error': 'Productor no encontrado'}), 404
    return jsonify(dict(result))

# ============================================================================
# RUTAS - ESTADÍSTICAS
# ============================================================================

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Obtener estadísticas generales del sistema"""
    stats = {}

    # Total de artistas
    result = execute_query("SELECT COUNT(*) as count FROM artist", fetch_one=True)
    stats['total_artists'] = result['count'] if result else 0

    # Total de álbumes
    result = execute_query("SELECT COUNT(*) as count FROM album", fetch_one=True)
    stats['total_albums'] = result['count'] if result else 0

    # Total de canciones
    result = execute_query("SELECT COUNT(*) as count FROM song", fetch_one=True)
    stats['total_songs'] = result['count'] if result else 0

    # Total de hits
    result = execute_query("SELECT COUNT(*) as count FROM song WHERE is_hit = TRUE", fetch_one=True)
    stats['total_hits'] = result['count'] if result else 0

    # Streams totales
    result = execute_query("SELECT SUM(streams_count) as total FROM song", fetch_one=True)
    stats['total_streams'] = result['total'] if result and result['total'] else 0

    # Artistas activos
    result = execute_query("""
        SELECT COUNT(DISTINCT a.artist_id) as count
        FROM artist a
        JOIN contract c ON a.artist_id = c.artist_id AND c.status = 'active'
    """, fetch_one=True)
    stats['active_artists'] = result['count'] if result else 0

    return jsonify(stats)

# ============================================================================
# RUTAS - SALUD DEL API
# ============================================================================

@app.route('/api/health', methods=['GET'])
def health():
    """Verificar estado del API"""
    conn = get_db_connection()
    if conn:
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    else:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500

@app.route('/', methods=['GET'])
def index():
    """Página de inicio del API"""
    return jsonify({
        'message': 'API REST - Gestión de Hits Musicales en Discográfica',
        'version': '1.0',
        'endpoints': {
            'artists': '/api/artists',
            'albums': '/api/albums',
            'songs': '/api/songs',
            'producers': '/api/producers',
            'stats': '/api/stats',
            'health': '/api/health'
        }
    })

# ============================================================================
# MANEJO DE ERRORES
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint no encontrado'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Error interno del servidor'}), 500

# ============================================================================
# EJECUCIÓN
# ============================================================================

if __name__ == '__main__':
    logger.info("Iniciando servidor Flask...")
    app.run(debug=True, host='0.0.0.0', port=5000)
