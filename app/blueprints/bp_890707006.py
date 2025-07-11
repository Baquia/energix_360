from flask import Blueprint, render_template, session
from app.utils import login_required_custom

bp_890707006 = Blueprint('bp_890707006', __name__)

@bp_890707006.route('/890707006.html')
@login_required_custom
def panel_pollosgar():
    return render_template('890707006.html', nombre=session.get('nombre'), empresa=session.get('empresa'))
