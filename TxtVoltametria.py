

import os

# ff = r'AgNO3_HOPG_3mM_20mv_1_cv.DTA'

def Dat_voltametria_to_txt(ff,name2):
    # print("Entro")
    print(name2)
    # name2 = name2[0]
    name2 = os.path.normpath(name2) 
    print(name2)
    path1 = os.path.normpath(ff) 
    print(path1)
    
    path= path1.split(os.sep)
    name3 = path[-1]
    
    name  = path[-1].replace('.DTA','')
    print(name,name3)
    
    dat =[]
    with open(ff) as f:
        for line in f:
            dat.append(line)

    dat = dat[61:] # inicia los datos 
    print(dat[0])
    print("Datos")
    # Name_file = os.path.join(os.sep, "home", "manolo", "miscosas", "fichero.txt")
    # print(name2)
    Name_file= name2+'\\'+name+'_M.txt'
    # print(name2)
    # print("????"*10)
    # print(Name_file)
    
    # Name_file = name+'_M.txt'
    with open(Name_file, 'w') as temp_file:
        for item in dat:
            temp_file.write("%s" % item)
    # if Name_file in os.listdir("."):
    #     print("\nArchivo creado en la ruta: \n\n\t{0}/{1}".format(os.getcwd(), Name_file))
    # else:
    #     print("El archivo no fue creado!!!\n")
# Dat_voltametria_to_txt(ff)
    return Name_file

if __name__=="__main__":
    DIRECTORIO_BASE = os.path.dirname(__file__)
    # print(DIRECTORIO_BASE)
    DIRECTORIO_ANALIZADO = os.listdir(DIRECTORIO_BASE)
    print(DIRECTORIO_ANALIZADO)
    D_hopgcr = os.path.join(DIRECTORIO_BASE, DIRECTORIO_ANALIZADO[1])
    D_hopgcv= os.path.join(DIRECTORIO_BASE, DIRECTORIO_ANALIZADO[1])
    D_itocr= os.path.join(DIRECTORIO_BASE, DIRECTORIO_ANALIZADO[3])
    D_itocv= os.path.join(DIRECTORIO_BASE, DIRECTORIO_ANALIZADO[4])

    direc1 = os.listdir(D_hopgcv)
    direc2 = os.listdir(D_itocv)
    # print(direc1)
    D_hopgcv_paths= [os.path.join(D_hopgcv,i) for i in direc1]
    D_itocv_paths= [os.path.join(D_itocv,i) for i in direc2]
    print(D_hopgcv_paths)
    Data1mM =os.listdir(D_hopgcv_paths[0])
    Data3mM =os.listdir(D_hopgcv_paths[1])
    Data5mM =os.listdir(D_hopgcv_paths[2])

    # print(Data1mM)
    # print("*"*4)
    Mol = [i for i in range(6) if i%2!=0 ]
    # print(Mol)
    for i in range(len(Mol)):
        src = direc1[i]+str(Mol[i])+'mM'
        src = D_hopgcv_paths[i]+'\\'+str(Mol[i])+'mM_M'
        # print(src)
        carpeta =os.path.exists(src)
        # print(carpeta)
        if carpeta ==False:
            for i in range(1,len(D_hopgcv_paths)+1):
                os.makedirs(D_hopgcv_paths[i-1]+'\\'+str(Mol[i-1])+'mM_M')
        else:
            print(carpeta)

    # print(type(D_hopgcv_paths[0]))
    file2 = str(Mol[0])+'mM_M'
    file3 = str(Mol[1])+'mM_M'
    file4 = str(Mol[2])+'mM_M'
    base= D_hopgcv_paths[0]
    base3= D_hopgcv_paths[1]
    base5= D_hopgcv_paths[2]
    D_1mMcv_M = os.path.join(base,file2)
    D_3mMcv_M = os.path.join(base3,file3)
    D_5mMcv_M = os.path.join(base5,file4)

    # print(D_1mMcv_M)
    # print(D_3mMcv_M)
    # print(D_5mMcv_M)

    data = Data1mM[1:]
    data3 = Data3mM[1:]
    data5 = Data5mM[1:]
    # print(data)
    D_1mMcv_M_paths1= [os.path.join(base,i) for i in data]
    D_1mMcv_M_paths2= D_1mMcv_M
    D_3mMcv_M_paths1= [os.path.join(base3,i) for i in data3]
    D_3mMcv_M_paths2= D_3mMcv_M
    D_5mMcv_M_paths1= [os.path.join(base5,i) for i in data5]
    D_5mMcv_M_paths2= D_5mMcv_M


    names =[D_1mMcv_M_paths2]*len(D_1mMcv_M_paths1)
    list(map(Dat_voltametria_to_txt,D_1mMcv_M_paths1,names))

    # print(D_3mMcv_M_paths1,D_3mMcv_M_paths2)
    names3 =[D_3mMcv_M_paths2]*len(D_1mMcv_M_paths1)
    list(map(Dat_voltametria_to_txt,D_3mMcv_M_paths1,names3))

    names5 =[D_5mMcv_M_paths2]*len(D_5mMcv_M_paths1)
    list(map(Dat_voltametria_to_txt,D_5mMcv_M_paths1,names5))

