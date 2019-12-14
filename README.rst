``externalReferences`` bereinigen
=================================

.. todo::
  
   - UUIDs generieren fuer neu angelegte ``externalReference`` objekte


installation
------------

Man braucht leserecht auf das private repo https://github.com/jkatzwinkel/tla-datentransformation.git
oder einen der mirrors.

.. code-block:: bash

   pipenv install

auszerdem natuerlich ``COUCHDB_SERVER_URL`` und credentials in :file:`.env` angeben.


tests
^^^^^

.. code-block:: bash

   pipenv run pytest test.py
   pipenv run pytest --doctest-modules fix_external_references.py


vorgehen
--------

Workflow ist ungefaehr folgender:

Man benutzt :file:`fix_external_references.py` um sich alle externen referenzen aus der couchdb zu holen:

.. code-block:: bash

   pipenv run python fix_external_references.py 

Man kann auch eingrenzen welche corpora man untersuchen will:

.. code-block:: bash

   pipenv run python fix_external_references.py aaew_ths aaew_wlist

Die dabei eingesammelten externalReferences werden in eine JSON-datei geschrieben, deren dateinamen man
mit der option ``--stat-file=<fn>`` angeben kann (default ist :file:`ext_refs.json`). Die struktur, in welcher
darin die references abgespeichert sind ist ca folgende:

.. code-block:: json

   {
     'collection_name': {
       'provider1': {
         'type1': {
           'doc_id1': [
             'ext_ref1',
             'ext_ref2'
          ],
          'null': {
            'doc_id1': [
              'ext_ref3'
            ]
          }
        }
      }
    }
  }

Man findet also unter den namen der corpora die dort vorkommenden provider und darunter die types, mit denen diese
gemeinsam auftreten, und darunter die dokumenten IDs welche externalReferences mit den jeweils darueber stehenden
provider und type enthalten. Die werte in den den dokumenten-IDs zugeordneten listen sind die ``reference`` werte
der im dokument aufgefundenen externalReferences.

Man kann also durch kurzes druebergucken erkennen ob bei der verwendung eines types und/oder providers eine konsistente
form eingehalten wurde, in der die ``reference`` werte gesetzt wurden.

Mit diesem wissen schreibt man dann fix-funktionen direkt ins script :file:`fix_external_references.py`.
Deren namen muessen ca folgendem schema entsprechen::

    fix_(<praedikat>_<wert>){1,n}

Als praedikat kann man attribute des externalReference-schemas angeben, also ``type``, ``provider`` oder ``reference``.
Nach dem unterstrich steht ein wert. Namen von funktionen, die dieses schema einhalten, werden ausgewertet und die
sich dabei ergebenden kriterien bestimmen, ob eine funktion auf einer bestimmten externalReference ausgefuehrt wird.

Beispiel:

.. code-block:: python

   def fix_type_vega(ID, ref):
       if not ref.get('provider'):
           ref['provider'] = 'vega'
       return ref

Diese funktion ist ausgelegt fuer externalReferences, deren ``type`` attribut den wert ``vega`` hat. Man koennte noch
zusaetzlich als kriterium zufuegen, dasz der ``provider`` wert leer sein musz, und die funktion so nennen:

.. code-block:: python

   def fix_type_vega_provider_null(ID, ref):
       ref['provider'] = 'vega'
       return ref

Beim praedikat ``reference`` gibt es die besonderheit dasz man als kriterium entweder den wert ``null`` (i.e. :class:`None`)
angeben kann, oder aber einen regulaeren ausdruck. Diesen definiert man im dictionary ``_rex`` direkt in 
:file:`fix_external_references.py` und gibt ihm einen namen, den man dann im funktionsnamen verwenden kann.
Diese funktion wird dann nur fuer externalReferences ausgefuehrt deren ``reference`` wert von diesem regex erkannt wird.

Um sich die definierten fix functions und die scenarien in welchen sie aktiv werden koennen anzusehen, kann man machen:

.. code-block:: bash

   pipenv run python fix_external_references.py --list-fixes


