import os

# Define las extensiones de código que deseas incluir
EXTENSIONES_VALIDAS = {
    # Backend
    '.js', '.ts', '.py', '.go', '.rb', '.php', '.java', '.cs',
    # Frontend
    '.jsx', '.tsx', '.vue', '.html', '.css', '.scss',
    # Configuración
    '.json', '.yaml', '.yml'
}

# Carpetas que debes ignorar para no inflar el archivo
CARPETAS_A_IGNORAR = {
    'node_modules', '.git', '.next', 'dist', 'build', 
    '__pycache__', 'venv', 'env', '.expo', 'out'
}

def consolidar_codigo(ruta_raiz, archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8') as f_salida:
        for raiz, carpetas, archivos in os.walk(ruta_raiz):
            # Filtrar carpetas ignoradas sobre la marcha
            carpetas[:] = [c for c in carpetas if c not in CARPETAS_A_IGNORAR]
            
            for archivo in archivos:
                _, ext = os.path.splitext(archivo)
                if ext.lower() in EXTENSIONES_VALIDAS:
                    ruta_completa = os.path.join(raiz, archivo)
                    ruta_relativa = os.path.relpath(ruta_completa, ruta_raiz)
                    
                    # Escribir encabezado de estructura para el Gem
                    f_salida.write(f"\n\n{'='*80}\n")
                    f_salida.write(f"ARCHIVO: {ruta_relativa}\n")
                    f_salida.write(f"{'='*80}\n\n")
                    
                    try:
                        with open(ruta_completa, 'r', encoding='utf-8', errors='ignore') as f_entrada:
                            f_salida.write(f_entrada.read())
                    except Exception as e:
                        f_salida.write(f"// Error al leer el archivo: {str(e)}\n")

if __name__ == "__main__":
    # Ejecuta el script en el directorio actual
    consolidar_codigo('.', 'proyecto_completo.txt')
    print("¡Listo! Sube 'proyecto_completo.txt' a la sección de conocimiento de tu Gem.")