from flask_login import UserMixin

# app/models.py
from flask_login import UserMixin

class Usuario(UserMixin):
    def __init__(self, id, nombre, cedula, tipo, clase, rol, empresa_id):
        self.id = id
        self.nombre = nombre
        self.cedula = cedula
        self.tipo = tipo
        self.clase = clase
        self.rol = rol
        self.empresa_id = empresa_id

    def get_id(self):
        return str(self.id)

    def __repr__(self):
        return f"<Usuario {self.nombre} - {self.empresa_id}>"
