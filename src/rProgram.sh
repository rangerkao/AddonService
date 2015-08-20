echo start...
pwd

#setting libs path 
libDir=lib/* 
temp=.:

append(){ 
                temp=$temp":"$1 
} 

for file in $libDir;    do 
    append $file 
done 

java -cp $temp AddonService

echo finished