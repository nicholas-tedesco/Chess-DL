# README ------------------------------------------------------------------------------------------
# 
#   file: 03-train-model.py
#   goal: fit deep learning model to predict next game board state given current board state
# 
# -------------------------------------------------------------------------------------------------

# packages and data -------------------------------------------------------------------------------

import pandas as pd 
from sqlalchemy import create_engine, text 
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import SGD, Adam
from torch.utils.data import DataLoader, TensorDataset

# db_path = '../data/lichess.db'
# table_name = 'games'

# select_query = text(f'SELECT * FROM {table_name};')
# engine = create_engine(f'sqlite:///{db_path}')
# data = pd.read_sql(select_query, f'sqlite:///{db_path}', index_col='GameID')

data = pd.read_csv('../data/test.csv')

print('')
print('\tSuccessfully loaded chess game data.')
print(f'\t- Rows: {data.shape[0]:,}')
print(f'\t- Cols: {data.shape[1]}')

data['768'] = [0 if turn == 'White' else 1 for turn in data['768']]
data['769'] = data['769'].astype('category').cat.codes

X = data.drop(columns = '769')
y = data['769']

X = torch.tensor(X.values, dtype=torch.float32)
y = torch.tensor(y.values, dtype=torch.int64)


# neural network configuration --------------------------------------------------------------------

class CustomNN(nn.Module): 

    """ purpose: create custom NN architecture for training chess DL model. 
    """

    def __init__(self): 

        super().__init__() 

        self.layer1 = nn.Linear(769, 256)
        self.layer2 = nn.Linear(256, 1024)
        self.layer3 = nn.Linear(1024, 4096)


    def forward(self, x):
      
        x = self.layer1(x) 
        x = F.relu(x) 

        x = self.layer2(x) 
        x = F.relu(x) 

        x = self.layer3(x) 
        output = F.log_softmax(x, dim=1)

        return output 


# train model -------------------------------------------------------------------------------------

dataset = TensorDataset(X, y)
dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

model = CustomNN() 

optimizer = Adam(model.parameters(), lr=0.01)
loss_fn = nn.NLLLoss()

for epoch in range(1): 

    print(f'\rCurrent Epoch: {epoch}', end='')

    for batch_X, batch_y in dataloader: 

        optimizer.zero_grad()
        model.train()

        output = model(batch_X)
        loss = loss_fn(output, batch_y) 
        
        loss.backward()
        optimizer.step() 

print(loss)


