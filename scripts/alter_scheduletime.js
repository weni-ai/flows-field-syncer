// iterate over all docs and alter schedule_time to schedule_times
function updateScheduleTimes() {
  // iterate over all docs on collection
  db.syncerconf.find().forEach(doc => {
    // Verificar se o campo schedule_time existe e é uma string
    if (doc.sync_rules && typeof doc.sync_rules.schedule_time === 'string') {
        // Transformar schedule_time em um array com um único elemento
        doc.sync_rules.schedule_times = [doc.sync_rules.schedule_time];
        // Remover o campo schedule_time
        delete doc.sync_rules.schedule_time;
        // Atualizar o documento na coleção
        db.syncerconf.updateOne({ _id: doc._id }, { $set: { "sync_rules.schedule_times": doc.sync_rules.schedule_times } }); 
    }
  });
  db.syncerconf.updateMany({}, { $unset: { "sync_rules.schedule_time": "" } });
}

updateScheduleTimes();

// how to run
// mongo ourdb --quiet update_schedule_times.js