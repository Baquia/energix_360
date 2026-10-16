# MÓDULO: UNIVERSAL | ARCHIVO: forms.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
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
        ('webmaster_admin_vehiculos', 'webmaster_admin_vehiculos'),
        # --- NUEVOS ROLES VENTAS Y DISTRIBUCIÓN (M3) ---
        ('controlador_bodegas', 'controlador_bodegas'),
        ('operador_logistica', 'operador_logistica'),
        ('verificador_bodegas', 'verificador_bodegas'),
        ('gerente_ventasdistribucion', 'gerente_ventasdistribucion'),
        ('controlador_ventas', 'controlador_ventas'),
        ('operador_ventas', 'operador_ventas'),
        ('controlador_administracion', 'controlador_administracion'),
        ('controlador_recurso_humano', 'controlador_recurso_humano'),
        ('operador_recursohumano', 'operador_recursohumano'),
        ('controlador_contabilidad', 'controlador_contabilidad'),
        ('operador_contable', 'operador_contable'),
        ('controlador_facturacion', 'controlador_facturacion'),
        ('operador_facturacion', 'operador_facturacion')
    ])
    password = PasswordField('Contraseña', validators=[InputRequired()])