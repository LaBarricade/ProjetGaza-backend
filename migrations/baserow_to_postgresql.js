/**
 * Created by PhpStorm.
 * User: Benoît Guchet
 * Date: 19/01/2026
 * Time: 21:06
 */
import fs from 'fs';
import Database from 'better-sqlite3';
import Client from 'pg-native';
import { execSync } from 'child_process';

/** FICHIER JSON EXPORTÉ DE BASEROW, PROJET COMPLET **/

//const sourceFilePath = 'source-data/migration 20260100/export_b9717722-9924-4fb5-8045-7d077b758dc3/database__7291bbe3ec8c4783bf61a9d4c0473f06_476027d5455f3b30fe0d3c7df867e15c53ecc16b1d73e57c2df32e490b93a8b6.json'
const sourceFilePath = 'source-data/migration 20260120/database__776606b9107d4af4b7ebe69fae43c6c2_4f0adcf8fe53b4cb84611e5a645c038c5c222f2a64c94ba561db22195b518014.json'
const dbUrl = 'postgresql://postgres:6k1StOu7K0uK3RLH@db.aemsrwloiirgovavfzdb.supabase.co:5432/postgres';
const client = new Client();
client.connectSync(dbUrl);


function normalize_symbol(name) {
  return name.replace(/ /g, '_').replace(/é/g, 'e').toLowerCase();
}
function dbQuery(sql, params = []) {
    console.log('SQL : ', sql, params);
    const res = client.querySync(sql, params);
    return res;
}

/**
 * Convert Baserow JSON export to SQLite database
 */
function convertBaserowToDb(jsonFile) {
  // Read JSON
  const raw = fs.readFileSync(jsonFile, 'utf-8');
  const database = JSON.parse(raw);
  const tables = database.tables;
  let totalRows = 0;
  let sql, insertStmt;
  const speakers = {};

  const mainTableData = tables[0];
  const ppOptionsData = mainTableData.fields[4].select_options;
  const sourcesOptionsData = mainTableData.fields[8].select_options;
  const tagsOptionsData = mainTableData.fields[10].select_options;

  console.log('Creating schema...');
  execSync(`psql -f ./create_db.sql ${dbUrl}`);
  console.log('OK');

  console.log('Inserting Sources, Tags, Parties...');
  client.prepareSync('insert_declaration_tag', 'INSERT INTO declarations_tags (declaration_id, tag_id, created_on) ' +
        'VALUES ($1, $2, NOW())');

  client.prepareSync('insert_speaker_row', 'INSERT INTO personnalites (prenom, nom, parti_politique_id, ville, departement, region, fonction, created_on)' +
        'VALUES ($1, $2, $3, $4, $5, $6, $7, NOW()) RETURNING id');

  client.prepareSync('insert_pp', `INSERT INTO Partis_Politiques (id, nom, color, created_on)
      VALUES ($1, $2, $3, NOW())`);
  ppOptionsData.forEach(row => client.executeSync('insert_pp', [row.id, row.value, row.color]));

  client.prepareSync('insert_sources', 'INSERT INTO Sources (id, nom, color, created_on) VALUES ($1, $2, $3, NOW())' +
      'ON CONFLICT (nom)   DO NOTHING');
  sourcesOptionsData.forEach(row =>  client.executeSync('insert_sources', [row.id, row.value, row.color]));

  client.prepareSync('insert_tags', 'INSERT INTO Tags (id, nom, color, created_on) VALUES ($1, $2, $3, NOW())');
  tagsOptionsData.forEach(row =>  client.executeSync('insert_tags', [row.id, row.value, row.color]));

  console.log('OK')
  console.log('Inserting Quotes & Personalities & News...');


  for (const table of tables) {
    const tableName = normalize_symbol(table.name);
    if (tableName === 'partis_politique' || tableName === 'personnalites')
      continue;

    const fields = table.fields;
    const rows = table.rows;
    let keepFieldsList;

    // Metadata columns
    /*columns.push(
      'id INTEGER PRIMARY KEY',
      '"order" REAL',
      'created_on timestamp',
      'updated_on timestamp',
      'created_by TEXT',
      'last_modified_by TEXT'
    );*/

    // Recreate table
    //dbQuery(`CREATE TABLE IF NOT EXISTS "${tableName}" (${columns.join(', ')})`);

    if (tableName === 'declarations') {
      keepFieldsList = {
        'citation': 'field_5055146',
        'date': 'field_5055147',
        'source_id': 'field_5055148',
        'lien': 'field_5055149',
        'collecteur': 'field_5055151',
        'commentaire': 'field_5055152',
        'est_publie': 'field_5623108',
        'personnalite_id': 'field_6810629',
        'id': 'id',
        'created_on': 'created_on',
        'updated_on': 'updated_on',
        'created_by': 'created_by',
        'last_modified_by': 'last_modified_by',
      };
    }
    else if (tableName === 'actualites') {
      keepFieldsList = {
        'texte': 'field_6308591',
        'est_publie': 'field_6308592',
        'date': 'field_6309006',
        'id': 'id',
        'created_on': 'created_on',
        'updated_on': 'updated_on',
        'created_by': 'created_by',
        'last_modified_by': 'last_modified_by',
      };
    }

    // Prepare INSERT once per table
    //const fieldNames = fields.map((f) => normalize_symbol(f.name));
    const fieldNames = Object.keys(keepFieldsList);

    /*fieldNames.push(
      'id',
      '"order"',
      'created_on',
      'updated_on',
      'created_by',
      'last_modified_by'
    );*/

    const placeholders = fieldNames.map((n, i) => '$' + (i + 1)).join(',');
    const insertSql = `
      INSERT INTO "${tableName}" (${fieldNames.join(',')})
      VALUES (${placeholders})
      RETURNING id;
    `;

    client.prepareSync('insert_row_' + tableName, insertSql);

    // Insert rows
    for (const row of rows) {
      let speakerId = null;
      if (tableName === 'declarations') {
        //-- Créer Personnalité ?
        const speakerLastName = row.field_5623110;
        const speakerFirstName = row.field_5623109;
        const id = speakerFirstName + ' ' + speakerLastName;

        if (speakerFirstName || speakerLastName) {
          //const existingSpeaker = dbQuery('SELECT id FROM personnalites WHERE nom = $1 AND prenom = $2', [speakerLastName, speakerFirstName]).at(0);
          const existingSpeaker = speakers[speakerLastName + ' ' + speakerFirstName];

          if (existingSpeaker) {
            speakerId = existingSpeaker.id;
          }
          else {
            const speakerData =  {
              id: row.id,
              prenom: speakerFirstName,
              nom: speakerLastName,
              ville: row.field_5055141,
              departement: row.field_5055142,
              region: row.field_5055143,
              fonction: row.field_5055145,
              parti_politique: row.field_5055144 || null
            };

            speakerId = client.executeSync('insert_speaker_row', [speakerData.prenom, speakerData.nom, speakerData.parti_politique,
                speakerData.ville, speakerData.departement, speakerData.region, speakerData.fonction]).at(0).id;

            speakers[speakerLastName + ' ' + speakerFirstName] = {id: speakerId};
          }
        }
      }

      const values = [];

      for (const field of fields) {
        const dataFieldKey = `field_${field.id}`;
        let value = row[dataFieldKey];
        if (!Object.values(keepFieldsList).includes(dataFieldKey))
          continue;

        if (tableName === 'declarations' && field.id === 6810629) {
          value = speakerId || null;
        }

        if (
          (field.type === 'multiple_select' || field.type === 'link_row') &&
          Array.isArray(value)
        ) {
          value = JSON.stringify(value);
        } else if (field.type === 'boolean') {
          value = value === true || value === 'true' ? 1 : 0;
        }

        values.push(value);
      }

      // Metadata values
      values.push(
        row.id,
        row.created_on,
        row.updated_on,
        row.created_by,
        row.last_modified_by
      );

      const rowId = client.executeSync('insert_row_' + tableName, values)[0].id;

      //-- Insert tags
      if (tableName === 'declarations') {
        const tags = row.field_5055150;
        tags.forEach(tag_id => client.executeSync('insert_declaration_tag', [rowId, tag_id]));
      }

      totalRows++;
    }
  }
  console.log('OK')


  console.log(`Database created successfully`);
  console.log(`Tables created: ${tables.length}`);
  console.log(`Rows inserted: ${totalRows}`);
}

// Usage
convertBaserowToDb(sourceFilePath);