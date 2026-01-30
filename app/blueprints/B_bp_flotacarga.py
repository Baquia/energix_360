from flask import Blueprint, render_template, session, redirect

# Definimos el Blueprint
bp_flotacarga = Blueprint('flotacarga', __name__)

@bp_flotacarga.route('/flota')
def home():
    return render_template('B_dashboard_tc.html')