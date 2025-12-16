-- ============================================================================
-- CONSULTAS DE PRUEBA - GESTIÓN DE HITS MUSICALES EN DISCOGRÁFICA
-- ============================================================================

-- ============================================================================
-- CONSULTA 1: Canciones Exitosas (Hits) con Información Completa
-- Descripción: Obtener todas las canciones que son hits, con detalles del álbum,
-- artistas participantes y número de plataformas de distribución
-- Complejidad: JOIN 4 tablas, GROUP BY, HAVING
-- ============================================================================

SELECT
  s.song_id,
  s.title AS cancion,
  a.title AS album,
  p.name AS productor,
  s.streams_count,
  COUNT(DISTINCT sd.distribution_id) AS num_plataformas,
  STRING_AGG(DISTINCT d.platform_name, ', ') AS plataformas,
  s.created_at
FROM song s
JOIN album a ON s.album_id = a.album_id
JOIN producer p ON a.producer_id = p.producer_id
LEFT JOIN song_distribution sd ON s.album_id = sd.album_id AND s.song_id = sd.song_id AND sd.status = 'active'
LEFT JOIN distribution d ON sd.distribution_id = d.distribution_id
WHERE s.is_hit = TRUE
GROUP BY s.song_id, s.title, a.title, p.name, s.streams_count, s.created_at
ORDER BY s.streams_count DESC
LIMIT 20;

-- ============================================================================
-- CONSULTA 2: Productores Más Activos con Estadísticas
-- Descripción: Ranking de productores por número de álbumes producidos,
-- sesiones de grabación realizadas y experiencia
-- Complejidad: JOIN 3 tablas, GROUP BY, agregaciones múltiples
-- ============================================================================

SELECT
  p.producer_id,
  p.name,
  p.specialty,
  p.years_experience,
  COUNT(DISTINCT a.album_id) AS num_albumes,
  COUNT(DISTINCT rs.session_id) AS num_sesiones,
  ROUND(AVG(rs.cost)::NUMERIC, 2) AS costo_promedio_sesion,
  MAX(a.release_date) AS ultimo_album
FROM producer p
LEFT JOIN album a ON p.producer_id = a.producer_id
LEFT JOIN recording_session rs ON p.producer_id = rs.producer_id
GROUP BY p.producer_id, p.name, p.specialty, p.years_experience
HAVING COUNT(DISTINCT a.album_id) > 0
ORDER BY num_albumes DESC, num_sesiones DESC
LIMIT 15;

-- ============================================================================
-- CONSULTA 3: Análisis de Ingresos por Plataforma de Distribución
-- Descripción: Calcular ingresos potenciales por plataforma considerando
-- comisiones, número de canciones y streams totales
-- Complejidad: JOIN 4 tablas, GROUP BY, CASE, agregaciones
-- ============================================================================

SELECT
  d.platform_name,
  d.agreement_status,
  COUNT(DISTINCT sd.song_id) AS num_canciones_activas,
  SUM(s.streams_count) AS streams_totales,
  d.commission_percentage,
  ROUND((SUM(s.streams_count) * (d.commission_percentage / 100.0))::NUMERIC, 2) AS ingreso_estimado,
  CASE
    WHEN d.agreement_status = 'active' THEN 'Operativo'
    WHEN d.agreement_status = 'pending' THEN 'Pendiente'
    ELSE 'Inactivo'
  END AS estado_acuerdo
FROM distribution d
LEFT JOIN song_distribution sd ON d.distribution_id = sd.distribution_id AND sd.status = 'active'
LEFT JOIN song s ON sd.album_id = s.album_id AND sd.song_id = s.song_id
GROUP BY d.distribution_id, d.platform_name, d.agreement_status, d.commission_percentage
ORDER BY streams_totales DESC NULLS LAST;

-- ============================================================================
-- CONSULTA 4: Artistas Activos con Contratos y Desempeño
-- Descripción: Listar artistas con contratos activos, mostrando información
-- de royalties, álbumes y canciones exitosas
-- Complejidad: JOIN 5 tablas, subconsultas, agregaciones
-- ============================================================================

SELECT
  a.artist_id,
  a.name AS artista,
  a.genre,
  c.royalty_percentage,
  c.end_date,
  COUNT(DISTINCT aa.album_id) AS num_albumes,
  COUNT(DISTINCT s.song_id) AS num_canciones,
  SUM(CASE WHEN s.is_hit THEN 1 ELSE 0 END) AS num_hits,
  ROUND(AVG(s.streams_count)::NUMERIC, 0) AS streams_promedio,
  MAX(s.streams_count) AS max_streams
FROM artist a
JOIN contract c ON a.artist_id = c.artist_id AND c.status = 'active'
LEFT JOIN artist_album aa ON a.artist_id = aa.artist_id
LEFT JOIN album alb ON aa.album_id = alb.album_id
LEFT JOIN song s ON alb.album_id = s.album_id
WHERE c.end_date >= CURRENT_DATE
GROUP BY a.artist_id, a.name, a.genre, c.royalty_percentage, c.end_date
ORDER BY num_hits DESC, streams_promedio DESC;

-- ============================================================================
-- CONSULTA 5: Sesiones de Grabación con Detalles Completos
-- Descripción: Análisis detallado de sesiones de grabación incluyendo
-- artista, productor, estudio, costo y fecha
-- Complejidad: JOIN 4 tablas, filtros temporales, ordenamiento múltiple
-- ============================================================================

SELECT
  rs.session_id,
  rs.session_date,
  rs.start_time,
  CONCAT(rs.duration_hours, ' horas') AS duracion,
  a.name AS artista,
  p.name AS productor,
  st.name AS estudio,
  st.location,
  rs.cost,
  ROUND((rs.cost / rs.duration_hours)::NUMERIC, 2) AS costo_por_hora,
  EXTRACT(YEAR FROM rs.session_date) AS ano,
  EXTRACT(MONTH FROM rs.session_date) AS mes
FROM recording_session rs
JOIN artist a ON rs.artist_id = a.artist_id
JOIN producer p ON rs.producer_id = p.producer_id
JOIN studio st ON rs.studio_id = st.studio_id
WHERE rs.session_date >= CURRENT_DATE - INTERVAL '1 year'
ORDER BY rs.session_date DESC, rs.start_time DESC
LIMIT 50;

-- ============================================================================
-- CONSULTA 6: Validación de Restricciones - Canciones Hit sin Distribución
-- Descripción: Identificar canciones marcadas como hit que NO tienen
-- distribuciones activas (violación de restricción de inclusividad)
-- Complejidad: LEFT JOIN, WHERE NOT EXISTS, validación de integridad
-- ============================================================================

SELECT
  s.song_id,
  s.album_id,
  s.title,
  s.streams_count,
  s.is_hit,
  COUNT(sd.distribution_id) AS num_distribuciones_activas,
  CASE
    WHEN s.is_hit = TRUE AND COUNT(sd.distribution_id) = 0 THEN 'VIOLACIÓN: Hit sin distribución'
    WHEN s.is_hit = TRUE AND s.streams_count < 1000000 THEN 'VIOLACIÓN: Marcado como hit pero sin suficientes streams'
    ELSE 'OK'
  END AS estado_validacion
FROM song s
LEFT JOIN song_distribution sd ON s.album_id = sd.album_id AND s.song_id = sd.song_id AND sd.status = 'active'
GROUP BY s.song_id, s.album_id, s.title, s.streams_count, s.is_hit
HAVING s.is_hit = TRUE AND COUNT(sd.distribution_id) = 0
ORDER BY s.streams_count DESC;

-- ============================================================================
-- CONSULTA 7: Resumen Ejecutivo - KPIs Principales
-- Descripción: Métricas clave del negocio: total de artistas, álbumes,
-- canciones, hits, ingresos estimados y personal
-- Complejidad: Múltiples agregaciones, UNION, subconsultas
-- ============================================================================

SELECT
  'Artistas Activos' AS metrica,
  COUNT(DISTINCT a.artist_id)::TEXT AS valor
FROM artist a
JOIN contract c ON a.artist_id = c.artist_id AND c.status = 'active'

UNION ALL

SELECT
  'Total Álbumes',
  COUNT(*)::TEXT
FROM album

UNION ALL

SELECT
  'Total Canciones',
  COUNT(*)::TEXT
FROM song

UNION ALL

SELECT
  'Canciones Hit',
  COUNT(*)::TEXT
FROM song
WHERE is_hit = TRUE

UNION ALL

SELECT
  'Plataformas de Distribución Activas',
  COUNT(*)::TEXT
FROM distribution
WHERE agreement_status = 'active'

UNION ALL

SELECT
  'Sesiones de Grabación (Último Año)',
  COUNT(*)::TEXT
FROM recording_session
WHERE session_date >= CURRENT_DATE - INTERVAL '1 year'

UNION ALL

SELECT
  'Streams Totales (Millones)',
  ROUND((SUM(streams_count) / 1000000.0)::NUMERIC, 2)::TEXT
FROM song

UNION ALL

SELECT
  'Costo Total Sesiones (Último Año)',
  ROUND(SUM(cost)::NUMERIC, 2)::TEXT
FROM recording_session
WHERE session_date >= CURRENT_DATE - INTERVAL '1 year';


-- ============================================================================
-- CONSULTA DE PRUEBA: UPDATE (Simulación de "Hit" Viral)
-- Descripción: Simula que una canción específica se vuelve viral, actualizando
-- sus reproducciones a 1.5 millones y marcándola como 'hit'.
-- Objetivo: Verificar que la canción aparezca posteriormente en los reportes de Hits.
-- Tablas afectadas: song
-- ============================================================================

UPDATE song
SET
    streams_count = 1500000,
    is_hit = TRUE
WHERE song_id = (
    -- Subconsulta para elegir una canción que NO sea hit y tenga pocas views
    SELECT song_id
    FROM song
    WHERE is_hit = FALSE AND streams_count < 5000
    LIMIT 1
);

-- Verificación rápida (Opcional)
-- SELECT * FROM song WHERE streams_count = 1500000;


-- ============================================================================
-- CONSULTA DE PRUEBA: DELETE (Limpieza de Contratos Vencidos)
-- Descripción: Elimina los contratos que finalizaron hace más de 5 años
-- y que ya tienen el estado marcado como 'expired' o 'inactive'.
-- Objetivo: Probar la integridad referencial y limpiar datos históricos obsoletos.
-- Tablas afectadas: contract
-- ============================================================================

DELETE FROM contract
WHERE status IN ('expired', 'inactive')
  AND end_date < CURRENT_DATE - INTERVAL '5 years';

-- Nota: Si esta consulta falla, significa que hay otras tablas (como pagos
-- o historial) que dependen de estos contratos, lo cual es correcto
-- en un diseño robusto (Integridad Referencial).




-- ============================================================================
-- CONSULTA ADICIONAL: Validación de Exclusividad de Contratos
-- Descripción: Verificar que no existan artistas con múltiples contratos activos
-- (violación de restricción de exclusividad)
-- Complejidad: GROUP BY, HAVING, validación de regla de negocio
-- ============================================================================

SELECT
  a.artist_id,
  a.name,
  COUNT(DISTINCT c.contract_id) AS num_contratos_activos,
  STRING_AGG(DISTINCT CONCAT(c.contract_id, ' (', c.start_date, ' - ', c.end_date, ')'), ', ') AS contratos
FROM artist a
JOIN contract c ON a.artist_id = c.artist_id AND c.status = 'active'
GROUP BY a.artist_id, a.name
HAVING COUNT(DISTINCT c.contract_id) > 1
ORDER BY num_contratos_activos DESC;

-- ============================================================================
-- CONSULTA ADICIONAL: Análisis de Género Musical
-- Descripción: Distribución de canciones, hits y streams por género musical
-- Complejidad: JOIN 2 tablas, GROUP BY, agregaciones múltiples
-- ============================================================================

SELECT
  a.genre,
  COUNT(DISTINCT a.album_id) AS num_albumes,
  COUNT(DISTINCT s.song_id) AS num_canciones,
  SUM(CASE WHEN s.is_hit THEN 1 ELSE 0 END) AS num_hits,
  ROUND(AVG(s.streams_count)::NUMERIC, 0) AS streams_promedio,
  SUM(s.streams_count) AS streams_totales,
  ROUND((SUM(CASE WHEN s.is_hit THEN 1 ELSE 0 END)::NUMERIC / COUNT(DISTINCT s.song_id) * 100), 2) AS porcentaje_hits
FROM album a
LEFT JOIN song s ON a.album_id = s.album_id
GROUP BY a.genre
ORDER BY streams_totales DESC NULLS LAST;

