-- ============================================================================
-- PROYECTO: Gestión de Nuevos Hits Musicales en una Discográfica
-- ASIGNATURA: Administración y Diseño de Bases de Datos
-- SGBD: PostgreSQL
-- ============================================================================

-- ============================================================================
-- 1. CREACIÓN DE DOMINIOS PERSONALIZADOS
-- ============================================================================

CREATE DOMAIN contract_status_domain AS VARCHAR(20)
  CHECK (VALUE IN ('active', 'inactive', 'suspended'));

CREATE DOMAIN album_status_domain AS VARCHAR(20)
  CHECK (VALUE IN ('draft', 'released', 'archived'));

CREATE DOMAIN song_status_domain AS VARCHAR(20)
  CHECK (VALUE IN ('active', 'inactive', 'pending'));

CREATE DOMAIN distribution_status_domain AS VARCHAR(20)
  CHECK (VALUE IN ('active', 'inactive', 'pending'));

CREATE DOMAIN contract_lifecycle_domain AS VARCHAR(20)
  CHECK (VALUE IN ('active', 'expired', 'terminated'));

CREATE DOMAIN percentage_domain AS NUMERIC(5,2)
  CHECK (VALUE >= 0 AND VALUE <= 100);

CREATE DOMAIN genre_domain AS VARCHAR(50)
  CHECK (VALUE IN ('pop', 'rock', 'hip-hop', 'jazz', 'classical', 'electronic', 'reggae', 'country', 'folk', 'other'));

-- ============================================================================
-- 2. CREACIÓN DE TABLAS
-- ============================================================================

-- Tabla: ARTIST
CREATE TABLE artist (
  artist_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  genre genre_domain NOT NULL,
  contract_date DATE NOT NULL,
  contract_status contract_status_domain NOT NULL DEFAULT 'active',
  biography TEXT,
  nationality VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_artist_name_nationality UNIQUE (name, nationality)
);

-- Tabla: PRODUCER
CREATE TABLE producer (
  producer_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  specialty VARCHAR(100) NOT NULL,
  years_experience INT NOT NULL CHECK (years_experience >= 0),
  email VARCHAR(255) NOT NULL UNIQUE,
  phone VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: STUDIO
CREATE TABLE studio (
  studio_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  location VARCHAR(255) NOT NULL,
  capacity INT NOT NULL CHECK (capacity > 0),
  equipment TEXT,
  hourly_rate NUMERIC(10,2) NOT NULL CHECK (hourly_rate > 0),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_studio_name_location UNIQUE (name, location)
);

-- Tabla: ALBUM
CREATE TABLE album (
  album_id SERIAL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  release_date DATE NOT NULL,
  producer_id INT NOT NULL REFERENCES producer(producer_id) ON DELETE RESTRICT,
  status album_status_domain NOT NULL DEFAULT 'draft',
  genre genre_domain NOT NULL,
  label_name VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_album_title_release_date UNIQUE (title, release_date)
);

-- Tabla: SONG (Entidad Débil)
CREATE TABLE song (
  album_id INT NOT NULL REFERENCES album(album_id) ON DELETE CASCADE,
  song_id INT NOT NULL,
  title VARCHAR(255) NOT NULL,
  duration INT NOT NULL CHECK (duration > 0),
  composer VARCHAR(255) NOT NULL,
  streams_count BIGINT NOT NULL DEFAULT 0 CHECK (streams_count >= 0),
  is_hit BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (album_id, song_id),
  CONSTRAINT ck_hit_streams CHECK (is_hit = FALSE OR streams_count >= 1000000)
);

-- Tabla: RECORDING_SESSION (Relación Ternaria)
CREATE TABLE recording_session (
  session_id SERIAL PRIMARY KEY,
  artist_id INT NOT NULL REFERENCES artist(artist_id) ON DELETE RESTRICT,
  producer_id INT NOT NULL REFERENCES producer(producer_id) ON DELETE RESTRICT,
  studio_id INT NOT NULL REFERENCES studio(studio_id) ON DELETE RESTRICT,
  session_date DATE NOT NULL,
  start_time TIME NOT NULL,
  duration_hours NUMERIC(5,2) NOT NULL CHECK (duration_hours > 0),
  cost NUMERIC(10,2) NOT NULL CHECK (cost >= 0),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: STAFF
CREATE TABLE staff (
  staff_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  salary NUMERIC(10,2) NOT NULL CHECK (salary > 0),
  department VARCHAR(100) NOT NULL,
  hire_date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: ENGINEER (Especialización IS_A)
CREATE TABLE engineer (
  engineer_id INT PRIMARY KEY REFERENCES staff(staff_id) ON DELETE CASCADE,
  certification VARCHAR(255) NOT NULL,
  technical_skills TEXT,
  studio_id INT NOT NULL REFERENCES studio(studio_id) ON DELETE RESTRICT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: MANAGER (Especialización IS_A)
CREATE TABLE manager (
  manager_id INT PRIMARY KEY REFERENCES staff(staff_id) ON DELETE CASCADE,
  commission_percentage percentage_domain NOT NULL,
  artists_managed INT NOT NULL DEFAULT 0 CHECK (artists_managed >= 0),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: DISTRIBUTION
CREATE TABLE distribution (
  distribution_id SERIAL PRIMARY KEY,
  platform_name VARCHAR(255) NOT NULL UNIQUE,
  commission_percentage percentage_domain NOT NULL,
  agreement_status distribution_status_domain NOT NULL DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla: SONG_DISTRIBUTION (Relación M:M)
CREATE TABLE song_distribution (
  song_id INT NOT NULL,
  album_id INT NOT NULL,
  distribution_id INT NOT NULL REFERENCES distribution(distribution_id) ON DELETE RESTRICT,
  publish_date DATE NOT NULL,
  status song_status_domain NOT NULL DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (song_id, album_id, distribution_id),
  FOREIGN KEY (album_id, song_id) REFERENCES song(album_id, song_id) ON DELETE CASCADE
);

-- Tabla: CONTRACT
CREATE TABLE contract (
  contract_id SERIAL PRIMARY KEY,
  artist_id INT NOT NULL REFERENCES artist(artist_id) ON DELETE CASCADE,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  royalty_percentage percentage_domain NOT NULL,
  contract_terms TEXT,
  status contract_lifecycle_domain NOT NULL DEFAULT 'active',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT ck_contract_dates CHECK (start_date <= end_date),
  CONSTRAINT uk_contract_artist_start_date UNIQUE (artist_id, start_date)
);

-- Tabla: ARTIST_ALBUM (Relación M:M)
CREATE TABLE artist_album (
  artist_id INT NOT NULL REFERENCES artist(artist_id) ON DELETE CASCADE,
  album_id INT NOT NULL REFERENCES album(album_id) ON DELETE CASCADE,
  role VARCHAR(100) DEFAULT 'featured',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (artist_id, album_id)
);

-- ============================================================================
-- 3. ÍNDICES PARA OPTIMIZACIÓN
-- ============================================================================

CREATE INDEX idx_album_producer_id ON album(producer_id);
CREATE INDEX idx_album_status ON album(status);
CREATE INDEX idx_song_album_id ON song(album_id);
CREATE INDEX idx_song_is_hit ON song(is_hit);
CREATE INDEX idx_song_streams_count ON song(streams_count);
CREATE INDEX idx_recording_session_artist_id ON recording_session(artist_id);
CREATE INDEX idx_recording_session_producer_id ON recording_session(producer_id);
CREATE INDEX idx_recording_session_studio_id ON recording_session(studio_id);
CREATE INDEX idx_recording_session_date ON recording_session(session_date);
CREATE INDEX idx_engineer_studio_id ON engineer(studio_id);
CREATE INDEX idx_song_distribution_distribution_id ON song_distribution(distribution_id);
CREATE INDEX idx_contract_artist_id ON contract(artist_id);
CREATE INDEX idx_contract_status ON contract(status);
CREATE INDEX idx_artist_album_album_id ON artist_album(album_id);

-- ============================================================================
-- 4. FUNCIONES Y TRIGGERS
-- ============================================================================

-- Función: Validar que un artista no tenga dos contratos activos simultáneamente
CREATE OR REPLACE FUNCTION check_active_contracts()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.status = 'active' THEN
    IF EXISTS (
      SELECT 1 FROM contract
      WHERE artist_id = NEW.artist_id
        AND status = 'active'
        AND contract_id != NEW.contract_id
    ) THEN
      RAISE EXCEPTION 'El artista ya tiene un contrato activo. Solo se permite un contrato activo por artista.';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_active_contracts
BEFORE INSERT OR UPDATE ON contract
FOR EACH ROW
EXECUTE FUNCTION check_active_contracts();

-- Función: Validar que una canción hit tenga distribuciones activas
CREATE OR REPLACE FUNCTION check_hit_distributions()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.is_hit = TRUE THEN
    IF NOT EXISTS (
      SELECT 1 FROM song_distribution
      WHERE album_id = NEW.album_id
        AND song_id = NEW.song_id
        AND status = 'active'
    ) THEN
      RAISE EXCEPTION 'Una canción hit debe tener al menos una distribución activa.';
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_hit_distributions
BEFORE INSERT OR UPDATE ON song
FOR EACH ROW
EXECUTE FUNCTION check_hit_distributions();

-- Función: Validar que productor no sea ingeniero en la misma sesión
CREATE OR REPLACE FUNCTION check_producer_engineer_conflict()
RETURNS TRIGGER AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM recording_session rs
    JOIN engineer e ON rs.studio_id = e.studio_id
    WHERE rs.session_id = NEW.session_id
      AND rs.producer_id = e.engineer_id
  ) THEN
    RAISE EXCEPTION 'El productor no puede ser ingeniero en la misma sesión de grabación.';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_check_producer_engineer_conflict
BEFORE INSERT OR UPDATE ON recording_session
FOR EACH ROW
EXECUTE FUNCTION check_producer_engineer_conflict();

-- Función: Actualizar timestamp de updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para actualizar updated_at en todas las tablas
CREATE TRIGGER trg_update_artist_timestamp
BEFORE UPDATE ON artist
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_producer_timestamp
BEFORE UPDATE ON producer
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_studio_timestamp
BEFORE UPDATE ON studio
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_album_timestamp
BEFORE UPDATE ON album
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_song_timestamp
BEFORE UPDATE ON song
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_recording_session_timestamp
BEFORE UPDATE ON recording_session
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_staff_timestamp
BEFORE UPDATE ON staff
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_engineer_timestamp
BEFORE UPDATE ON engineer
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_manager_timestamp
BEFORE UPDATE ON manager
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_distribution_timestamp
BEFORE UPDATE ON distribution
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_update_contract_timestamp
BEFORE UPDATE ON contract
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

-- Función: Calcular costo de sesión basado en tarifa del estudio
CREATE OR REPLACE FUNCTION calculate_session_cost()
RETURNS TRIGGER AS $$
DECLARE
  studio_rate NUMERIC;
BEGIN
  SELECT hourly_rate INTO studio_rate FROM studio WHERE studio_id = NEW.studio_id;
  NEW.cost := studio_rate * NEW.duration_hours;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_calculate_session_cost
BEFORE INSERT OR UPDATE ON recording_session
FOR EACH ROW
EXECUTE FUNCTION calculate_session_cost();

-- ============================================================================
-- 5. VISTAS ÚTILES
-- ============================================================================

-- Vista: Canciones Exitosas (Hits)
CREATE VIEW v_hit_songs AS
SELECT
  s.song_id,
  s.album_id,
  s.title AS song_title,
  a.title AS album_title,
  s.streams_count,
  s.composer,
  COUNT(DISTINCT sd.distribution_id) AS num_platforms
FROM song s
JOIN album a ON s.album_id = a.album_id
LEFT JOIN song_distribution sd ON s.album_id = sd.album_id AND s.song_id = sd.song_id
WHERE s.is_hit = TRUE
GROUP BY s.song_id, s.album_id, s.title, a.title, s.streams_count, s.composer;

-- Vista: Productores Más Activos
CREATE VIEW v_active_producers AS
SELECT
  p.producer_id,
  p.name,
  p.specialty,
  COUNT(DISTINCT a.album_id) AS num_albums,
  COUNT(DISTINCT rs.session_id) AS num_sessions
FROM producer p
LEFT JOIN album a ON p.producer_id = a.producer_id
LEFT JOIN recording_session rs ON p.producer_id = rs.producer_id
GROUP BY p.producer_id, p.name, p.specialty;

-- Vista: Artistas por Contrato Activo
CREATE VIEW v_active_artists AS
SELECT
  a.artist_id,
  a.name,
  a.genre,
  c.contract_id,
  c.start_date,
  c.end_date,
  c.royalty_percentage
FROM artist a
JOIN contract c ON a.artist_id = c.artist_id
WHERE c.status = 'active' AND c.end_date >= CURRENT_DATE;

-- Vista: Ingresos por Plataforma de Distribución
CREATE VIEW v_distribution_revenue AS
SELECT
  d.platform_name,
  COUNT(DISTINCT sd.song_id) AS num_songs,
  SUM(s.streams_count) AS total_streams,
  d.commission_percentage
FROM distribution d
LEFT JOIN song_distribution sd ON d.distribution_id = sd.distribution_id AND sd.status = 'active'
LEFT JOIN song s ON sd.album_id = s.album_id AND sd.song_id = s.song_id
GROUP BY d.distribution_id, d.platform_name, d.commission_percentage;

