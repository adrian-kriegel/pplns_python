
#!/bin/sh

for f in "dev/hooks/*"
do
  
  rm ".git/hooks/$(basename $f)"
  ln $f ".git/hooks/$(basename $f)"

done

echo "Installed git hooks."
