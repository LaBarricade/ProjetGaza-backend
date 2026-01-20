/**
 * Created by PhpStorm.
 * User: Benoît Guchet
 * Date: 19/01/2026
 * Time: 21:06
 */

import fs from 'fs';
import Client from 'pg-native';
import {parse as parseCsv} from "csv-parse/sync";

const client = new Client();
client.connectSync('postgresql://postgres:6k1StOu7K0uK3RLH@db.aemsrwloiirgovavfzdb.supabase.co:5432/postgres');


/**
 * 1/ Récupérer le fichier ici : https://data.assemblee-nationale.fr/acteurs/deputes-en-exercice
 * 2/ "Fichier CSV - Liste des députés en format Libre Office"
 * 3/ Renommer les champs comme suit :
 * "id","firstname","lastname","region","department","num_circo","job","political_group_long","political_group_short"
"795998","Émeline","K/Bidi","Réunion","Réunion","4","Avocate","Gauche Démocrate et Républicaine","GDR"
"796106","Édouard","Bénard","Normandie","Seine-Maritime","3","Collaborateur d'élu","Gauche Démocrate et Républicaine","GDR"
 */
const sourceFile = 'source-data/liste-politiques-20260119/liste_deputes_libre_office.csv';


function dbQuery(sql, params = []) {
    console.log('SQL : ', sql, params);
    const res = client.querySync(sql, params);
    return res;
}

//"id","firstname","lastname","region","department","num_circo","job","political_group_long","political_group_short"

function insertAllDeputies(csvFile) {
    const rawCsv = fs.readFileSync(csvFile, 'utf-8');

    const array = parseCsv(rawCsv, {
        columns: true,
        skip_empty_lines: true,
        delimiter: ",",
    });

    array.forEach(row => {

        const pp_id = dbQuery(`INSERT INTO Partis_Politiques (nom, nom_court, created_on)
              VALUES ($1, $2, NOW())
              ON CONFLICT (nom) DO UPDATE SET nom = EXCLUDED.nom 
              RETURNING id`, [
                  'Groupe ' + row.political_group_long,
            'Groupe ' + row.political_group_short,
        ])?.at(0)?.id;
        /*client.prepareSync('insert_declaration_tag', 'INSERT INTO declarations_tags (declaration_id, tag_id, created_on) ' +
              'VALUES ($1, $2, NOW())');
        client.executeSync('insert_declaration_tag', [rowId, tag_id])*/
        const extraInfos = JSON.stringify({job: row.job, political_group_long: row.political_group_long, political_group_short: row.political_group_short, num_circo: row.num_circo});
        dbQuery(`INSERT INTO personnalites (prenom, nom, parti_politique_id, ville, departement, region, fonction, extra_infos, created_on)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW()) 
                ON CONFLICT (nom, prenom) DO UPDATE SET extra_infos = $8, departement = $5, region = $6
            RETURNING id`, [
                row.firstname,
                row.lastname,
                pp_id,
                null,
                row.department,
                row.region,
                `Député.e (${row.department}, ${row.num_circo}e circonscription)`,
                extraInfos
        ]);
    })

    console.log(array[0], array[1]);
}

insertAllDeputies(sourceFile);