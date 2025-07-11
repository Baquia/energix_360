# forms.py
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SelectField
from wtforms.validators import InputRequired

class LoginForm(FlaskForm):
    cedula = StringField('Cédula', validators=[InputRequired()])
    password = PasswordField('Contraseña', validators=[InputRequired()])
    empresa = SelectField('Empresa', choices=[], validators=[InputRequired()])

class RegistroUsuarioForm(FlaskForm):
    nombre = StringField('Nombre', validators=[InputRequired()])
    cedula = StringField('Cédula', validators=[InputRequired()])
    empresa_id = StringField('Empresa ID', validators=[InputRequired()])
    tipo = SelectField('Tipo', choices=[('cliente', 'cliente'), ('webmaster', 'webmaster')])
    clase = SelectField('Clase', choices=[('op', 'op'), ('admin', 'admin')])
    rol = SelectField('Rol', choices=[
        ('clientes_op_gas', 'clientes_op_gas'),
        ('clientes_op_electricidad', 'clientes_op_electricidad'),
        ('clientes_op_vehiculos', 'clientes_op_vehiculos'),
        ('clientes_admin_gas', 'clientes_admin_gas'),
        ('clientes_admin_electricidad', 'clientes_admin_electricidad'),
        ('clientes_admin_vehiculos', 'clientes_admin_vehiculos'),
        ('webmaster_op_gas', 'webmaster_op_gas'),
        ('webmaster_op_electricidad', 'webmaster_op_electricidad'),
        ('webmaster_op_vehiculos', 'webmaster_op_vehiculos'),
        ('webmaster_admin_gas', 'webmaster_admin_gas'),
        ('webmaster_admin_electricidad', 'webmaster_admin_electricidad'),
        ('webmaster_admin_vehiculos', 'webmaster_admin_vehiculos')
    ])
    password = PasswordField('Contraseña', validators=[InputRequired()])
