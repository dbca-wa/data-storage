import tempfile
import os
import json

from .utils import remove_folder,JSONEncoder,JSONDecoder
from .resource import ResourceConstant


def change_metaindex(repository_metadata,f_metaname_code):
    """
    Change the index calculating logic of the indexed resource repository
    repository_metadata: the current resource repository's metadata
    f_metaname_code: the new source code to calculate a resource's metaname
    """
    work_dir = tempfile.mkdtemp()
    try:
        #save all existing resource metadatas to a json file
        with open(os.path.join(work_dir,"resource_metadatas.json"),'w') as f:
            for res_metadata in repository_metadata.resource_metadatas(throw_exception=False,resource_status=ResourceConstant.ALL_RESOURCE,resource_file=None):
                f.write(json.dumps(res_metadata,cls=JSONEncoder))
                f.write(os.linesep)

        #meta index file
        meta_dir = os.path.join(work_dir,"metadata")
        os.mkdir(meta_dir)
        repository_metadata.download(os.path.join(meta_dir,"{}.json".format(repository_metadata._metaname)))
        #download all meta data file
        for metaname,filename in repository_metadata.json:
            repository_metadata.create_metadata_client(metaname).download(os.path.join(meta_dir,os.path.split(filename)[1]))

        #remove meta file
        for metaname in [o[0] for o in repository_metadata.json]:
            repository_metadata.create_metadata_client(metaname).delete()

        #remove meta index file
        repository_metadata.delete()


        #create a new repository metadata
        keywords = dict((key,getattr(repository_metadata,attr)) for key,attr in repository_metadata.meta_metadata_kwargs)
        keywords["f_metaname_code"] = f_metaname_code
        new_repository_metadata = repository_metadata.__class__(repository_metadata._storage,**keywords)
        with open(os.path.join(work_dir,"resource_metadatas.json"),'r') as f:
            while True:
                data = f.readline()
                if not data:
                    break
                data = data.strip()
                if not data:
                    continue
                res_metadata = json.loads(data.strip(),cls=JSONDecoder)
                new_repository_metadata.update_resource(res_metadata)

        remove_folder(work_dir)
        return new_repository_metadata

    except Exception as ex:
        print("Failed to change the metadata index, check the folder({}) to get the previous meta data".format(work_dir))
        
    

        

    
