
    Martinique
    French Guiana
    French Polynesia
    Guadeloupe
    Réunion
    Mayotte
    New Caledonia

    Puerto Rico
    Guam
    United States Virgin Islands U.S. Virgin Islands

    Hong Kong
    Macau -> Macau

loop this {word} and find address from validated_addresses $match: {address: {$regex: "Macau",$options: "i"}}
loop validated_addresses(address,country)
    address = trim from start to word 
        example : 輝旺閣 Edifício Fai Wong Kok, 7-9, 賣草地街 Rua da Palha, 中區 Centro, Sé, Macau, 519000, China
        change: 輝旺閣 Edifício Fai Wong Kok, 7-9, 賣草地街 Rua da Palha, 中區 Centro, Sé, Macau
    country = word
    check look_like_address, validate_address_region 
        true: update with this address and country
        false: delete         
        use batch no do action once (use update many, delete many)
espacialy:
    in case: Réunion
        address: replace Réunion -> Reunion
        country: Reunion
        1, Chemin des Faucons, Les Lianes, Saint-Joseph, Saint-Pierre, Réunion, 97480, France
        trim: 1, Chemin des Faucons, Les Lianes, Saint-Joseph, Saint-Pierre, Réunion
        replace: 1, Chemin des Faucons, Les Lianes, Saint-Joseph, Saint-Pierre, Reunion
    in case: United States Virgin Islands
        address: repace United States Virgin Islands -> U.S. Virgin Islands
        country: U.S. Virgin Islands
    in case: Macau
        address: add Macao
            輝旺閣 Edifício Fai Wong Kok, 7-9, 賣草地街 Rua da Palha, 中區 Centro, Sé, Macau, 519000, China
            trim: 輝旺閣 Edifício Fai Wong Kok, 7-9, 賣草地街 Rua da Palha, 中區 Centro, Sé, Macau
            add: 輝旺閣 Edifício Fai Wong Kok, 7-9, 賣草地街 Rua da Palha, 中區 Centro, Sé, Macau, Macao
        country: Macao